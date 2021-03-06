package hex.tree;

import com.google.common.util.concurrent.AtomicDouble;
import sun.misc.Unsafe;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.nbhm.UtilUnsafe;
import water.util.*;

import java.util.Arrays;
import java.util.Random;

/** A Histogram, computed in parallel over a Vec.
 *
 *  <p>A {@code DHistogram} bins every value added to it, and computes a the
 *  vec min and max (for use in the next split), and response mean and variance
 *  for each bin.  {@code DHistogram}s are initialized with a min, max and
 *  number-of- elements to be added (all of which are generally available from
 *  a Vec).  Bins run from min to max in uniform sizes.  If the {@code
 *  DHistogram} can determine that fewer bins are needed (e.g. boolean columns
 *  run from 0 to 1, but only ever take on 2 values, so only 2 bins are
 *  needed), then fewer bins are used.
 *  
 *  <p>{@code DHistogram} are shared per-node, and atomically updated.  There's
 *  an {@code add} call to help cross-node reductions.  The data is stored in
 *  primitive arrays, so it can be sent over the wire.
 *  
 *  <p>If we are successively splitting rows (e.g. in a decision tree), then a
 *  fresh {@code DHistogram} for each split will dynamically re-bin the data.
 *  Each successive split will logarithmically divide the data.  At the first
 *  split, outliers will end up in their own bins - but perhaps some central
 *  bins may be very full.  At the next split(s) - if they happen at all -
 *  the full bins will get split, and again until (with a log number of splits)
 *  each bin holds roughly the same amount of data.  This 'UniformAdaptive' binning
 *  resolves a lot of problems with picking the proper bin count or limits -
 *  generally a few more tree levels will equal any fancy but fixed-size binning strategy.
 *
 *  <p>Support for histogram split points based on quantiles (or random points) is
 *  available as well, via {@code _histoType}.
 *
*/
public final class DHistogram extends Iced {
  public final transient String _name; // Column name (for debugging)
  public final double _minSplitImprovement;
  public final byte  _isInt;    // 0: float col, 1: int col, 2: categorical & int col
  public char  _nbin;     // Bin count (excluding NA bucket)
  public double _step;     // Linear interpolation step per bin
  public final double _min, _maxEx; // Conservative Min/Max over whole collection.  _maxEx is Exclusive.
  public double _w[];           // weighted count of observations per bin, shared, atomically incremented
  private double _wY[], _wYY[]; // weighted response per bin and weighted squared response per bin, shared, atomically incremented
  private AtomicDouble _wNA, _wYNA, _wYYNA; // same for missing observations

  // Atomically updated double min/max
  protected    double  _min2, _maxIn; // Min/Max, shared, atomically updated.  _maxIn is Inclusive.
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  static private final long _min2Offset;
  static private final long _max2Offset;
  static {
    try {
      _min2Offset = _unsafe.objectFieldOffset(DHistogram.class.getDeclaredField("_min2"));
      _max2Offset = _unsafe.objectFieldOffset(DHistogram.class.getDeclaredField("_maxIn"));
    } catch( Exception e ) {
      throw H2O.fail();
    }
  }

  public SharedTreeModel.SharedTreeParameters.HistogramType _histoType; //whether ot use random split points
  public transient double _splitPts[]; // split points between _min and _maxEx (either random or based on quantiles)
  public final long _seed;
  public transient boolean _hasQuantiles;
  public Key _globalQuantilesKey; //key under which original top-level quantiles are stored;

  static class HistoQuantiles extends Keyed<HistoQuantiles> {
    public HistoQuantiles(Key<HistoQuantiles> key, double[] splitPts) {
      super(key);
      this.splitPts = splitPts;
    }
    double[/*nbins*/] splitPts;
  }


  public static int[] activeColumns(DHistogram[] hist) {
    int[] cols = new int[hist.length];
    int len=0;
    for( int i=0; i<hist.length; i++ ) {
      if (hist[i]==null) continue;
      assert hist[i]._min < hist[i]._maxEx && hist[i].nbins() > 1 : "broken histo range "+ hist[i];
      cols[len++] = i;        // Gather active column
    }
//    cols = Arrays.copyOfRange(cols, len, hist.length);
    return cols;
  }

  public void setMin( double min ) {
    long imin = Double.doubleToRawLongBits(min);
    double old = _min2;
    while( min < old && !_unsafe.compareAndSwapLong(this, _min2Offset, Double.doubleToRawLongBits(old), imin ) )
      old = _min2;
  }
  // Find Inclusive _max2
  public void setMaxIn( double max ) {
    long imax = Double.doubleToRawLongBits(max);
    double old = _maxIn;
    while( max > old && !_unsafe.compareAndSwapLong(this, _max2Offset, Double.doubleToRawLongBits(old), imax ) )
      old = _maxIn;
  }

  public DHistogram(String name, final int nbins, int nbins_cats, byte isInt, double min, double maxEx,
                    double minSplitImprovement, SharedTreeModel.SharedTreeParameters.HistogramType histogramType, long seed, Key globalQuantilesKey) {
    assert nbins > 1;
    assert nbins_cats > 1;
    assert maxEx > min : "Caller ensures "+maxEx+">"+min+", since if max==min== the column "+name+" is all constants";
    _isInt = isInt;
    _name = name;
    _min=min;
    _maxEx=maxEx;               // Set Exclusive max
    _min2 = Double.MAX_VALUE;   // Set min/max to outer bounds
    _maxIn= -Double.MAX_VALUE;
    _minSplitImprovement = minSplitImprovement;
    _histoType = histogramType;
    _seed = seed;
    while (_histoType == SharedTreeModel.SharedTreeParameters.HistogramType.RoundRobin) {
      SharedTreeModel.SharedTreeParameters.HistogramType[] h = SharedTreeModel.SharedTreeParameters.HistogramType.values();
      _histoType = h[(int)Math.abs(seed++ % h.length)];
    }
    if (_histoType== SharedTreeModel.SharedTreeParameters.HistogramType.AUTO)
      _histoType= SharedTreeModel.SharedTreeParameters.HistogramType.UniformAdaptive;
    assert(_histoType!= SharedTreeModel.SharedTreeParameters.HistogramType.RoundRobin);
    _globalQuantilesKey = globalQuantilesKey;
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    int xbins = isInt == 2 ? nbins_cats : nbins;
    if (isInt > 0 && maxEx - min <= xbins) {
      assert ((long) min) == min : "Overflow for integer/categorical histogram: minimum value cannot be cast to long without loss: (long)" + min + " != " + min + "!";                // No overflow
      xbins = (char) ((long) maxEx - (long) min);  // Shrink bins
      _step = 1.0f;                           // Fixed stepsize
    } else {
      _step = xbins / (maxEx - min);              // Step size for linear interpolation, using mul instead of div
      assert _step > 0 && !Double.isInfinite(_step) : "Histogram step size for column '" + name + "' is invalid: " + _step + ".";
    }
    _nbin = (char) xbins;
    assert(_nbin>0);
    assert(_w ==null);
    assert(_wY ==null);
    assert(_wYY ==null);
    // Do not allocate the big arrays here; wait for scoreCols to pick which cols will be used.
  }

  // Interpolate d to find bin#
  public int bin( double col_data ) {
//    assert( !Double.isNaN(col_data) ); //NAs go to a separate bucket
    if( Double.isNaN(col_data) ) return _w.length-1; // NAs go right, and for numeric features, this is consistent

    if (Double.isInfinite(col_data)) // Put infinity to most left/right bin
      if (col_data<0) return 0;
      else return _w.length-1;
    assert _min <= col_data && col_data < _maxEx : "Coldata " + col_data + " out of range " + this;
    // When the model is exposed to new test data, we could have data that is
    // out of range of any bin - however this binning call only happens during
    // model-building.
    int idx1;
    double pos = _hasQuantiles ? col_data : ((col_data - _min) * _step);
    if (_splitPts != null) {
      idx1 = Arrays.binarySearch(_splitPts, pos);
      if (idx1 < 0) idx1 = -idx1 - 2;
    } else {
      idx1 = (int) pos;
    }
    if (idx1 == _w.length) idx1--; // Roundoff error allows idx1 to hit upper bound, so truncate
    assert 0 <= idx1 && idx1 < _w.length : idx1 + " " + _w.length;
    return idx1;
  }
  public double binAt( int b ) {
    if (_hasQuantiles) return _splitPts[b];
    return _min + (_splitPts == null ? b : _splitPts[b]) / _step;
  }

  public int nbins() { return _nbin; }
  public double bins(int b) { return _w[b]; }

  // Big allocation of arrays
  public void init() {
    assert _w == null;
    if (_histoType==SharedTreeModel.SharedTreeParameters.HistogramType.Random) {
      // every node makes the same split points
      Random rng = RandomUtils.getRNG((Double.doubleToRawLongBits(((_step+0.324)*_min+8.3425)+89.342*_maxEx) + 0xDECAF*_nbin + 0xC0FFEE*_isInt + _seed));
      assert(_nbin>1);
      _splitPts = new double[_nbin];
      _splitPts[0] = 0;
      _splitPts[_nbin - 1] = _nbin-1;
      for (int i = 1; i < _nbin-1; ++i)
         _splitPts[i] = rng.nextFloat() * (_nbin-1);
      Arrays.sort(_splitPts);
    }
    else if (_histoType== SharedTreeModel.SharedTreeParameters.HistogramType.QuantilesGlobal) {
      assert (_splitPts == null);
      if (_globalQuantilesKey != null) {
        HistoQuantiles hq = DKV.getGet(_globalQuantilesKey);
        if (hq != null) {
          _splitPts = ((HistoQuantiles) DKV.getGet(_globalQuantilesKey)).splitPts;
          if (_splitPts!=null) {
//            Log.info("Obtaining global splitPoints: " + Arrays.toString(_splitPts));
            _splitPts = ArrayUtils.limitToRange(_splitPts, _min, _maxEx);
            if (_splitPts.length > 1 && _splitPts.length < _nbin)
              _splitPts = ArrayUtils.padUniformly(_splitPts, _nbin);
            if (_splitPts.length <= 1) {
              _splitPts = null; //abort, fall back to uniform binning
              _histoType = SharedTreeModel.SharedTreeParameters.HistogramType.UniformAdaptive;
            }
            else {
              _hasQuantiles=true;
              _nbin = (char)_splitPts.length;
//              Log.info("Refined splitPoints: " + Arrays.toString(_splitPts));
            }
          }
        }
      }
    }
    else assert(_histoType== SharedTreeModel.SharedTreeParameters.HistogramType.UniformAdaptive);
    //otherwise AUTO/UniformAdaptive
    assert(_nbin>0);
    _w = MemoryManager.malloc8d(_nbin);
    _wY = MemoryManager.malloc8d(_nbin);
    _wYY = MemoryManager.malloc8d(_nbin);
    _wNA = new AtomicDouble();
    _wYNA = new AtomicDouble();
    _wYYNA = new AtomicDouble();
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  void incr( double col_data, double y, double w ) {
//    assert !Double.isNaN(col_data);
    assert Double.isInfinite(col_data) || (_min <= col_data && col_data < _maxEx) : "col_data "+col_data+" out of range "+this;
    int b = bin(col_data);      // Compute bin# via linear interpolation
    water.util.AtomicUtils.DoubleArray.add(_w,b,w); // Bump count in bin
    // Track actual lower/upper bound per-bin
    if (!Double.isInfinite(col_data)) {
      setMin(col_data);
      setMaxIn(col_data);
    }
    if( y != 0 && w != 0) incr0(b,y,w);
  }

  // Merge two equal histograms together.  Done in a F/J reduce, so no
  // synchronization needed.
  public void add( DHistogram dsh ) {
    assert _isInt == dsh._isInt && _nbin == dsh._nbin && _step == dsh._step &&
      _min == dsh._min && _maxEx == dsh._maxEx;
    assert (_w == null && dsh._w == null) || (_w != null && dsh._w != null);
    if( _w == null ) return;
    ArrayUtils.add(_w,dsh._w);
    if( _min2 > dsh._min2  ) _min2 = dsh._min2;
    if( _maxIn < dsh._maxIn) _maxIn = dsh._maxIn;
    add0(dsh);
  }

  // Inclusive min & max
  public double find_min  () { return _min2 ; }
  public double find_maxIn() { return _maxIn; }
  // Exclusive max
  public double find_maxEx() { return find_maxEx(_maxIn,_isInt); }
  public static double find_maxEx(double maxIn, int isInt ) {
    double ulp = Math.ulp(maxIn);
    if( isInt > 0 && 1 > ulp ) ulp = 1;
    double res = maxIn+ulp;
    return Double.isInfinite(res) ? maxIn : res;
  }

  // The initial histogram bins are setup from the Vec rollups.
  public static DHistogram[] initialHist(Frame fr, int ncols, int nbins, DHistogram hs[], long seed, SharedTreeModel.SharedTreeParameters parms, Key[] globalQuantilesKey) {
    Vec vecs[] = fr.vecs();
    for( int c=0; c<ncols; c++ ) {
      Vec v = vecs[c];
      final double minIn = Math.max(v.min(),-Double.MAX_VALUE); // inclusive vector min
      final double maxIn = Math.min(v.max(), Double.MAX_VALUE); // inclusive vector max
      final double maxEx = find_maxEx(maxIn,v.isInt()?1:0);     // smallest exclusive max
      final long vlen = v.length();
      hs[c] = v.naCnt()==vlen || v.min()==v.max() ?
          null : make(fr._names[c],nbins, (byte)(v.isCategorical() ? 2 : (v.isInt()?1:0)), minIn, maxEx, seed, parms, globalQuantilesKey[c]);
      assert (hs[c] == null || vlen > 0);
    }
    return hs;
  }


  public static DHistogram make(String name, final int nbins, byte isInt, double min, double maxEx, long seed, SharedTreeModel.SharedTreeParameters parms, Key globalQuantilesKey) {
    return new DHistogram(name,nbins, parms._nbins_cats, isInt, min, maxEx, parms._min_split_improvement, parms._histogram_type, seed, globalQuantilesKey);
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_maxEx).append(" step=" + (1 / _step) + " nbins=" + nbins() + " isInt=" + _isInt);
    if( _w != null ) {
      for(int b = 0; b< _w.length; b++ ) {
        sb.append(String.format("\ncnt=%f, [%f - %f], mean/var=", _w[b],_min+b/_step,_min+(b+1)/_step));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
      }
      sb.append('\n');
    }
    return sb.toString();
  }
  double mean(int b) {
    double n = _w[b];
    return n>0 ? _wY[b]/n : 0;
  }

  /**
   * compute the sample variance within a given bin
   * @param b bin id
   * @return sample variance (>= 0)
   */
  public double var (int b) {
    double n = _w[b];
    if( n<=1 ) return 0;
    return Math.max(0, (_wYY[b] - _wY[b]* _wY[b]/n)/(n-1)); //not strictly consistent with what is done elsewhere (use n instead of n-1 to get there)
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute response mean & variance.
  // Done racily instead F/J map calls, so atomic
  public void incr0( int b, double y, double w ) {
    AtomicUtils.DoubleArray.add(_wY,b,(float)(w*y)); //See 'HistogramTest' JUnit for float-casting rationalization
    AtomicUtils.DoubleArray.add(_wYY,b,(float)(w*y*y));
  }
  // Same, except square done by caller
  public void incr1( int b, double y, double yy) {
    AtomicUtils.DoubleArray.add(_wY,b,(float)y); //See 'HistogramTest' JUnit for float-casting rationalization
    AtomicUtils.DoubleArray.add(_wYY,b,(float)yy);
  }

  // Merge two equal histograms together.
  // Done in a F/J reduce, so no synchronization needed.
  public void add0( DHistogram dsh ) {
    ArrayUtils.add(_wY,dsh._wY);
    ArrayUtils.add(_wYY,dsh._wYY);
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  public DTree.Split scoreMSE( int col, double min_rows) {
    final int nbins = nbins();
    assert nbins > 1;

    // Histogram arrays used for splitting, these are either the original bins
    // (for an ordered predictor), or sorted by the mean response (for an
    // unordered predictor, i.e. categorical predictor).
    double[] wY = _wY;
    double[] wYY = _wYY;
    double[] w = _w;
    int idxs[] = null;          // and a reverse index mapping

    // For categorical (unordered) predictors, sort the bins by average
    // prediction then look for an optimal split.  Currently limited to categoricals
    // where we're one-per-bin.  No point for 3 or fewer bins as all possible
    // combinations (just 3) are tested without needing to sort.
    if( _isInt == 2 && _step == 1.0f && nbins >= 4 ) {
      // Sort the index by average response
      idxs = MemoryManager.malloc4(nbins+1); // Reverse index
      for( int i=0; i<nbins+1; i++ ) idxs[i] = i;
      final double[] avgs = MemoryManager.malloc8d(nbins+1);
      for( int i=0; i<nbins; i++ ) avgs[i] = _w[i]==0 ? 0 : _wY[i]/ _w[i]; // Average response
      avgs[nbins] = Double.MAX_VALUE;
      ArrayUtils.sort(idxs, avgs);
      // Fill with sorted data.  Makes a copy, so the original data remains in
      // its original order.
      wY = MemoryManager.malloc8d(nbins);
      wYY = MemoryManager.malloc8d(nbins);
      w = MemoryManager.malloc8d(nbins);
      for( int i=0; i<nbins; i++ ) {
        wY[i] = _wY[idxs[i]];
        wYY[i] = _wYY[idxs[i]];
        w[i] = _w[idxs[i]];
      }
    }

    // Compute mean/var for cumulative bins from 0 to nbins inclusive.
    double wY0[] = MemoryManager.malloc8d(nbins+1);
    double wYY0[] = MemoryManager.malloc8d(nbins+1);
    double w0[] = MemoryManager.malloc8d(nbins+1);
    for( int b=1; b<=nbins; b++ ) {
      double m0 = wY0[b-1],  m1 = wY[b-1];
      double s0 = wYY0[b-1],  s1 = wYY[b-1];
      double k0 = w0[b-1],  k1 = w[b-1];
      if( k0==0 && k1==0 )
        continue;
      wY0[b] = m0+m1;
      wYY0[b] = s0+s1;
      w0[b] = k0+k1;
    }
    double tot = w0[nbins];
    // Is any split possible with at least min_obs?
    if( tot < 2*min_rows )
      return null;
    // If we see zero variance, we must have a constant response in this
    // column.  Normally this situation is cut out before we even try to split,
    // but we might have NA's in THIS column...
    double var = wYY0[nbins]*tot - wY0[nbins]* wY0[nbins];
    if( var == 0 ) {
//      assert isConstantResponse();
      return null;
    }
    // If variance is really small, then the predictions (which are all at
    // single-precision resolution), will be all the same and the tree split
    // will be in vain.
    if( ((float)var) == 0f )
      return null;

    // Compute mean/var for cumulative bins from nbins to 0 inclusive.
    double  w1[] = MemoryManager.malloc8d(nbins+1);
    double  wY1[] = MemoryManager.malloc8d(nbins+1);
    double wYY1[] = MemoryManager.malloc8d(nbins+1);
    for( int b=nbins-1; b>=0; b-- ) {
      double m0 =  wY1[b+1], m1 = wY[b];
      double s0 = wYY1[b+1], s1 = wYY[b];
      double k0 = w1 [b+1], k1 = w[b];
      if( k0==0 && k1==0 )
        continue;
       wY1[b] = m0+m1;
      wYY1[b] = s0+s1;
      w1 [b] = k0+k1;
      assert MathUtils.compare(w0[b]+w1[b],tot,1e-5,1e-5);
    }

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for categoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    int best=0;                         // The no-split
    double best_se0=Double.MAX_VALUE;   // Best squared error
    double best_se1=Double.MAX_VALUE;   // Best squared error
    byte equal=0;                       // Ranged check
    for( int b=1; b<=nbins-1; b++ ) {
      if( w[b] == 0 ) continue; // Ignore empty splits
      if( w0[b] < min_rows ) continue;
      if( w1[b] < min_rows ) break; // w1 shrinks at the higher bin#s, so if it fails once it fails always
      // We're making an unbiased estimator, so that MSE==Var.
      // Then Squared Error = MSE*N = Var*N
      //                    = (wYY/N - mean^2)*N
      //                    = wYY - N*mean^2
      //                    = wYY - N*(sum/N)(sum/N)
      //                    = wYY - sum^2/N
      double se0 = wYY0[b] - wY0[b]* wY0[b]/ w0[b];
      double se1 = wYY1[b] - wY1[b]* wY1[b]/w1[b];
      if( se0 < 0 ) se0 = 0;    // Roundoff error; sometimes goes negative
      if( se1 < 0 ) se1 = 0;    // Roundoff error; sometimes goes negative
      if( (se0+se1 < best_se0+best_se1) || // Strictly less error?
              // Or tied MSE, then pick split towards middle bins
              (se0+se1 == best_se0+best_se1 &&
                      Math.abs(b -(nbins>>1)) < Math.abs(best-(nbins>>1))) ) {
        best_se0 = se0;   best_se1 = se1;
        best = b;
      }
    }

    // If the bin covers a single value, we can also try an equality-based split
    if( _isInt > 0 && _step == 1.0f &&    // For any integral (not float) column
            _maxEx-_min > 2 && idxs==null ) { // Also need more than 2 (boolean) choices to actually try a new split pattern
      for( int b=1; b<=nbins-1; b++ ) {
        if( w[b] < min_rows ) continue; // Ignore too small splits
        double N = w0[b] + w1[b+1];
        if( N < min_rows )
          continue; // Ignore too small splits
        double wY2 =   wY0[b] +  wY1[b+1];
        double wYY2 = wYY0[b] + wYY1[b+1];
        double si =    wYY2 - wY2 * wY2 /   N   ; // Left+right, excluding 'b'
        double sx =    wYY[b] - wY[b]*wY[b]/w[b]; // Just 'b'
        if( si < 0 ) si = 0;    // Roundoff error; sometimes goes negative
        if( sx < 0 ) sx = 0;    // Roundoff error; sometimes goes negative
        if( si+sx < best_se0+best_se1 ) { // Strictly less error?
          best_se0 = si;   best_se1 = sx;
          best = b;        equal = 1; // Equality check
        }
      }
    }

    // For categorical (unordered) predictors, we sorted the bins by average
    // prediction then found the optimal split on sorted bins
    IcedBitSet bs = null;       // In case we need an arbitrary bitset
    if( idxs != null ) {        // We sorted bins; need to build a bitset
      int min=Integer.MAX_VALUE;// Compute lower bound and span for bitset
      int max=Integer.MIN_VALUE;
      for( int i=best; i<nbins; i++ ) {
        min=Math.min(min,idxs[i]);
        max=Math.max(max,idxs[i]);
      }
      bs = new IcedBitSet(max-min+1,min); // Bitset with just enough span to cover the interesting bits
      for( int i=best; i<nbins; i++ ) bs.set(idxs[i]); // Reverse the index then set bits
      equal = (byte)(bs.max() <= 32 ? 2 : 3); // Flag for bitset split; also check max size
    }

    if( best==0 ) return null;  // No place to split
    double se = wYY1[0] - wY1[0]* wY1[0]/w1[0]; // Squared Error with no split
    //if( se <= best_se0+best_se1) return null; // Ultimately roundoff error loses, and no split actually helped
    if (!(best_se0+best_se1 < se * (1- _minSplitImprovement))) return null; // Ultimately roundoff error loses, and no split actually helped
    double n0 = equal != 1 ?  w0[best] : w0[best]  + w1[best+1];
    double n1 = equal != 1 ? w1[best] : w[best]                ;
    double p0 = equal != 1 ? wY0[best] : wY0[best] + wY1[best+1];
    double p1 = equal != 1 ? wY1[best] : wY[best]               ;
    if( MathUtils.equalsWithinOneSmallUlp((float)(p0/n0),(float)(p1/n1)) ) return null; // No difference in predictions, which are all at 1 float ULP
    return new DTree.Split(col,best,bs,equal,se,best_se0,best_se1,n0,n1,p0/n0,p1/n1);
  }

  public void updateSharedHistosAndReset(double[] w, double[] wY, double[] wYY, double[] ws, double[] cs, double[] ys, int [] rows, int hi, int lo) {
    double minmax[] = new double[]{_min2,_maxIn};
    // Gather all the data for this set of rows, for 1 column and 1 split/NID
    // Gather min/max, wY and sum-squares.
    for(int r = lo; r< hi; ++r) {
      int k = rows[r];
      double weight = ws[k];
      if (weight == 0) continue;
      double col_data = cs[k];
      if( col_data < minmax[0] ) minmax[0] = col_data;
      if( col_data > minmax[1] ) minmax[1] = col_data;
      double y = ys[k];
      double wy = weight * y;
//      if (Double.isNaN(col_data)) {
//        //separate bucket for NA - atomically added to the shared histo
//        _wNA.addAndGet(weight);
//        _wYNA.addAndGet(wy);
//        _wYYNA.addAndGet(wy*y);
//      } else {
        // increment local pre-thread histograms
        int b = bin(col_data);
        w[b] += weight;
        wY[b] += wy;
        wYY[b] += wy * y;
//      }
    }

    // Atomically update histograms
    setMin(minmax[0]);       // Track actual lower/upper bound per-bin
    setMaxIn(minmax[1]);

    final int len = _w.length;
    for( int b=0; b<len; b++ ) { // Bump counts in weighted counts and reset the temp arrays to 0
      if( w[b] != 0 ) { AtomicUtils.DoubleArray.add(_w,b,w[b]); w[b]=0; }
    }
    for( int b=0; b<len; b++ ) { // Bump counts in weighted response and response^2 and reset the temp arrays to 0
      if( wY[b] != 0 || wYY[b] != 0 ) { incr1(b,wY[b],wYY[b]); wY[b]=wYY[b]=0; }
    }
  }

}
