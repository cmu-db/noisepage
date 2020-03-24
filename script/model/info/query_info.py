from type import OpUnit

_Q1_P1 = [(OpUnit.SCAN, [600000, 46, 600000]),
          (OpUnit.INT_GREATER, [600000]),
          (OpUnit.INT_ADD, [600000]),
          (OpUnit.REAL_ADD, [4800000]),
          (OpUnit.REAL_MULTIPLY, [1800000]),
          (OpUnit.AGG_BUILD, [600000, 2, 4]),
          ]

_Q1_P2 = [(OpUnit.AGG_PROBE, [4, 2, 4]),
          (OpUnit.SORT_BUILD, [4, 10, 4])
          ]

_Q1_Q3 = [(OpUnit.SORT_PROBE, [4, 10, 4]),
          ]

_Q4_P1 = [(OpUnit.SCAN, [150000, 16, 100]),
          (OpUnit.REAL_GREATER, [300000]),
          (OpUnit.JOIN_BUILD, [5357, 4, 5357]),
          ]

_Q4_P2 = [(OpUnit.SCAN, [600000, 12, 100]),
          (OpUnit.INT_GREATER, [600000]),
          (OpUnit.JOIN_PROBE, [366000, 4, 5357]),
          (OpUnit.AGG_BUILD, [5357, 8, 5]),
          (OpUnit.INT_ADD, [5357]),
          ]

_Q4_P3 = [(OpUnit.AGG_PROBE, [5, 8, 5]),
          (OpUnit.SORT_BUILD, [5, 8, 5])
          ]

_Q4_P4 = [(OpUnit.SORT_PROBE, [5, 8, 5]),
          ]

_Q5_P1 = [(OpUnit.SCAN, [5, 12, 5]),
          (OpUnit.INT_GREATER, [5]),
          (OpUnit.JOIN_BUILD, [1, 4, 1]),
          ]

_Q5_P2 = [(OpUnit.SCAN, [25, 18, 25]),
          (OpUnit.JOIN_PROBE, [25, 4, 5]),
          (OpUnit.JOIN_BUILD, [5, 4, 5]),
          ]

_Q5_P3 = [(OpUnit.SCAN, [15000, 8, 15000]),
          (OpUnit.JOIN_PROBE, [15000, 4, 3000]),
          (OpUnit.JOIN_BUILD, [3000, 4, 3000]),
          ]

_Q5_P4 = [(OpUnit.SCAN, [150000, 12, 150000]),
          (OpUnit.INT_GREATER, [300000]),
          (OpUnit.JOIN_PROBE, [150000, 4, 30000]),
          (OpUnit.JOIN_BUILD, [30000, 4, 30000]),
          ]

_Q5_P5 = [(OpUnit.SCAN, [1000, 8, 1000]),
          (OpUnit.JOIN_BUILD, [1000, 8, 1000]),
          ]

_Q5_P6 = [(OpUnit.SCAN, [600000, 24, 600000]),
          (OpUnit.JOIN_PROBE, [600000, 4, 120000]),
          (OpUnit.JOIN_PROBE, [120000, 4, 6000]),
          (OpUnit.AGG_BUILD, [6000, 4, 5]),
          (OpUnit.REAL_MULTIPLY, [6000]),
          (OpUnit.REAL_ADD, [12000]),
          ]

_Q5_P7 = [(OpUnit.AGG_PROBE, [5, 4, 5]),
          (OpUnit.SORT_BUILD, [5, 8, 5]),
          ]

_Q5_P8 = [(OpUnit.SORT_PROBE, [5, 8, 5]),
          ]

_Q6_P1 = [(OpUnit.SCAN, [600000, 24, 600000]),
          (OpUnit.REAL_GREATER, [1800000]),
          (OpUnit.INT_GREATER, [1200000]),
          (OpUnit.REAL_MULTIPLY, [4285]),
          (OpUnit.REAL_ADD, [4285]),
          ]

_Q7_P1 = [(OpUnit.SCAN, [25, 14, 25]),
          (OpUnit.REAL_GREATER, [50]),
          (OpUnit.SCAN, [50, 14, 50]),
          (OpUnit.REAL_GREATER, [100]),
          (OpUnit.JOIN_BUILD, [4, 4, 2]),
          ]

_Q7_P2 = [(OpUnit.SCAN, [15000, 8, 15000]),
          (OpUnit.JOIN_PROBE, [15000, 4, 1200]),
          (OpUnit.JOIN_BUILD, [1200, 4, 1200]),
          ]

_Q7_P3 = [(OpUnit.SCAN, [150000, 8, 150000]),
          (OpUnit.JOIN_PROBE, [150000, 4, 12000]),
          (OpUnit.JOIN_BUILD, [12000, 4, 12000]),
          ]

_Q7_P4 = [(OpUnit.SCAN, [1000, 8, 1000]),
          (OpUnit.JOIN_BUILD, [1000, 8, 1000]),
          ]

_Q7_P5 = [(OpUnit.SCAN, [600000, 28, 600000]),
          (OpUnit.INT_GREATER, [1200000]),
          (OpUnit.JOIN_PROBE, [170000, 4, 14000]),
          (OpUnit.JOIN_PROBE, [14000, 4, 350]),
          (OpUnit.AGG_BUILD, [350, 12, 8]),
          (OpUnit.REAL_MULTIPLY, [350]),
          (OpUnit.REAL_ADD, [700]),
          ]

_Q7_P6 = [(OpUnit.AGG_PROBE, [4, 12, 4]),
          (OpUnit.SORT_BUILD, [4, 12, 4]),
          ]

_Q7_P7 = [(OpUnit.SORT_PROBE, [4, 12, 4]),
          ]


_Q11_P1 = [(OpUnit.SCAN, [25, 14, 25]),
           (OpUnit.REAL_GREATER, [25]),
           (OpUnit.JOIN_BUILD, [1, 4, 1]),
           ]

_Q11_P2 = [(OpUnit.SCAN, [25, 14, 25]),
           (OpUnit.REAL_GREATER, [25]),
           (OpUnit.JOIN_BUILD, [1, 4, 1]),
           ]

_Q11_P3 = [(OpUnit.SCAN, [1000, 8, 1000]),
           (OpUnit.JOIN_PROBE, [1000, 4, 50]),
           (OpUnit.JOIN_BUILD, [50, 4, 50]),
           ]

_Q11_P4 = [(OpUnit.SCAN, [80000, 16, 80000]),
           (OpUnit.JOIN_PROBE, [80000, 4, 4000]),
           (OpUnit.REAL_MULTIPLY, [4000]),
           (OpUnit.REAL_ADD, [4000]),
           ]

_Q11_P5 = [(OpUnit.SCAN, [80000, 20, 80000]),
           (OpUnit.JOIN_PROBE, [80000, 4, 4000]),
           (OpUnit.REAL_MULTIPLY, [4000]),
           (OpUnit.REAL_ADD, [4000]),
           (OpUnit.AGG_BUILD, [4000, 4, 1000]),
           ]

_Q11_P6 = [(OpUnit.AGG_PROBE, [4000, 4, 4000]),
           (OpUnit.REAL_MULTIPLY, [4000]),
           (OpUnit.REAL_GREATER, [4000]),
           (OpUnit.SORT_BUILD, [2500, 4, 2500]),
           ]

_Q11_P7 = [(OpUnit.SORT_PROBE, [2500, 12, 2500]),
           ]


_SCAN_LINEITEM_P1 = [(OpUnit.SCAN, [600000, 58, 600000]),
                     ]

_SCAN_ORDERS_P1 = [(OpUnit.SCAN, [150000, 4, 150000]),
                   ]

FEATURE_MAP = {"tpch_q1_p1": _Q1_P1,
               "tpch_q1_p2": _Q1_P2,
               "tpch_q1_p3": _Q1_Q3,
               "tpch_q4_p1": _Q4_P1,
               "tpch_q4_p2": _Q4_P2,
               "tpch_q4_p3": _Q4_P3,
               "tpch_q4_p4": _Q4_P4,
               "tpch_q5_p1": _Q5_P1,
               "tpch_q5_p2": _Q5_P1,
               "tpch_q5_p3": _Q5_P3,
               "tpch_q5_p4": _Q5_P4,
               "tpch_q5_p5": _Q5_P5,
               "tpch_q5_p6": _Q5_P6,
               "tpch_q5_p7": _Q5_P7,
               "tpch_q5_p8": _Q5_P8,
               "tpch_q6_p1": _Q6_P1,
               "tpch_q7_p1": _Q7_P1,
               "tpch_q7_p2": _Q7_P1,
               "tpch_q7_p3": _Q7_P3,
               "tpch_q7_p4": _Q7_P4,
               "tpch_q7_p5": _Q7_P5,
               "tpch_q7_p6": _Q7_P6,
               "tpch_q7_p7": _Q7_P7,
               "tpch_q11_p1": _Q11_P1,
               "tpch_q11_p2": _Q11_P1,
               "tpch_q11_p3": _Q11_P3,
               "tpch_q11_p4": _Q11_P4,
               "tpch_q11_p5": _Q11_P5,
               "tpch_q11_p6": _Q11_P6,
               "tpch_q11_p7": _Q11_P7,
               "tpch_scan_lineitem_p1": _SCAN_LINEITEM_P1,
               "tpch_scan_orders_p1": _SCAN_ORDERS_P1,
               }
