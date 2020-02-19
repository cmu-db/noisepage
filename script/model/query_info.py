from type import Target, OpUnit, ArithmeticFeature

q1_p1 = [(OpUnit.SCAN, [600000, 50, 600000]),
         (OpUnit.INT_GREATER, [600000]),
         (OpUnit.INT_ADD, [2400000]),
         (OpUnit.REAL_ADD, [4800000]),
         (OpUnit.REAL_MULTIPLY, [2400000]),
         (OpUnit.AGG_BUILD, [600000, 4, 4]),
         ]

q1_p2 = [(OpUnit.AGG_PROBE, [4, 4, 4]),
         (OpUnit.SORT_BUILD, [4, 2, 4])
         ]

q1_p3 = [(OpUnit.SORT_PROBE, [4, 8, 4]),
         ]

q4_p1 = [(OpUnit.SCAN, [150000, 16, 100]),
         (OpUnit.REAL_GREATER, [300000]),
         (OpUnit.JOIN_BUILD, [5357, 4, 5357]),
         ]

q4_p2 = [(OpUnit.SCAN, [600000, 12, 100]),
         (OpUnit.INT_GREATER, [600000]),
         (OpUnit.JOIN_PROBE, [366000, 4, 5357]),
         (OpUnit.AGG_BUILD, [5357, 8, 5]),
         (OpUnit.INT_ADD, [5357]),
         ]

q4_p3 = [(OpUnit.AGG_PROBE, [5, 8, 5]),
         (OpUnit.SORT_BUILD, [5, 8, 5])
         ]

q6_p1 = [(OpUnit.SCAN, [600000, 24, 100]),
         (OpUnit.REAL_GREATER, [1200000]),
         (OpUnit.INT_GREATER, [1200000]),
         (OpUnit.REAL_MULTIPLY, [4285]),
         (OpUnit.REAL_ADD, [4285]),
         ]

feature_map = {"tpch_q1_p1": q1_p1,
               "tpch_q1_p2": q1_p2,
               "tpch_q1_p3": q1_p3,
               "tpch_q4_p1": q4_p1,
               "tpch_q4_p2": q4_p2,
               "tpch_q4_p3": q4_p3,
               "tpch_q6_p1": q6_p1
               }


