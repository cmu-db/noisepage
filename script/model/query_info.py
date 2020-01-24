from type import Target, OpUnit, ArithmeticFeature

q1_p1 = [(OpUnit.SCAN, [600000, 50, 100]),
         (OpUnit.INT_GREATER, [600000]),
         (OpUnit.INT_ADD, [2400000]),
         (OpUnit.REAL_ADD, [4800000]),
         (OpUnit.REAL_MULTIPLY, [2400000]),
         (OpUnit.AGG_BUILD, [600000, 4, 1]),  # Cardinality is less than 1%, but should in fact only be 4 in total
         ]

q1_p2 = [(OpUnit.AGG_PROBE, [4, 4, 100]),
         (OpUnit.SORT_BUILD, [4, 2, 100])
         ]

q1_p3 = [(OpUnit.SORT_PROBE, [4, 8, 100]),
         ]

feature_map = {"tpch_q1_p1": q1_p1,
               "tpch_q1_p2": q1_p2,
               "tpch_q1_p3": q1_p3}


