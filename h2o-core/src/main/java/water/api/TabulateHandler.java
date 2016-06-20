package water.api;

import water.api.schemas3.TabulateV3;
import water.util.Tabulate;

public class TabulateHandler extends Handler {

  public TabulateV3 run(int version, TabulateV3 spv3) {
    Tabulate sp = spv3.createAndFillImpl();
    return (TabulateV3) SchemaServer.schema(version, Tabulate.class).fillFromImpl(sp.execImpl());
  }
}