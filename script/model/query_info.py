from type import OpUnit

q1_p1 = [(OpUnit.SCAN, [600000, 46, 600000]),
         (OpUnit.INT_GREATER, [600000]),
         (OpUnit.INT_ADD, [600000]),
         (OpUnit.REAL_ADD, [4800000]),
         (OpUnit.REAL_MULTIPLY, [1800000]),
         (OpUnit.AGG_BUILD, [600000, 2, 4]),
         ]

q1_p2 = [(OpUnit.AGG_PROBE, [4, 2, 4]),
         (OpUnit.SORT_BUILD, [4, 10, 4])
         ]

q1_p3 = [(OpUnit.SORT_PROBE, [4, 10, 4]),
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

q4_p4 = [(OpUnit.SORT_PROBE, [5, 8, 5]),
         ]

q5_p1 = [(OpUnit.SCAN, [5, 12, 5]),
         (OpUnit.INT_GREATER, [5]),
         (OpUnit.JOIN_BUILD, [1, 4, 1]),
         ]

q5_p2 = [(OpUnit.SCAN, [25, 18, 25]),
         (OpUnit.JOIN_PROBE, [25, 4, 5]),
         (OpUnit.JOIN_BUILD, [5, 4, 5]),
         ]

q5_p3 = [(OpUnit.SCAN, [15000, 8, 15000]),
         (OpUnit.JOIN_PROBE, [15000, 4, 3000]),
         (OpUnit.JOIN_BUILD, [3000, 4, 3000]),
         ]

q5_p4 = [(OpUnit.SCAN, [150000, 12, 150000]),
         (OpUnit.INT_GREATER, [300000]),
         (OpUnit.JOIN_PROBE, [150000, 4, 30000]),
         (OpUnit.JOIN_BUILD, [30000, 4, 30000]),
         ]

q5_p5 = [(OpUnit.SCAN, [1000, 8, 1000]),
         (OpUnit.JOIN_BUILD, [1000, 8, 1000]),
         ]

q5_p6 = [(OpUnit.SCAN, [600000, 24, 600000]),
         (OpUnit.JOIN_PROBE, [600000, 4, 120000]),
         (OpUnit.JOIN_PROBE, [120000, 4, 6000]),
         (OpUnit.AGG_BUILD, [6000, 4, 5]),
         (OpUnit.REAL_MULTIPLY, [6000]),
         (OpUnit.REAL_ADD, [12000]),
         ]

q5_p7 = [(OpUnit.AGG_PROBE, [5, 4, 5]),
         (OpUnit.SORT_BUILD, [5, 8, 5]),
         ]

q5_p8 = [(OpUnit.SORT_PROBE, [5, 8, 5]),
         ]

q6_p1 = [(OpUnit.SCAN, [600000, 24, 600000]),
         (OpUnit.REAL_GREATER, [1800000]),
         (OpUnit.INT_GREATER, [1200000]),
         (OpUnit.REAL_MULTIPLY, [4285]),
         (OpUnit.REAL_ADD, [4285]),
         ]

q7_p1 = [(OpUnit.SCAN, [25, 14, 25]),
         (OpUnit.REAL_GREATER, [50]),
         (OpUnit.SCAN, [50, 14, 50]),
         (OpUnit.REAL_GREATER, [100]),
         (OpUnit.JOIN_BUILD, [4, 4, 2]),
         ]

q7_p2 = [(OpUnit.SCAN, [15000, 8, 15000]),
         (OpUnit.JOIN_PROBE, [15000, 4, 1200]),
         (OpUnit.JOIN_BUILD, [1200, 4, 1200]),
         ]

q7_p3 = [(OpUnit.SCAN, [150000, 8, 150000]),
         (OpUnit.JOIN_PROBE, [150000, 4, 12000]),
         (OpUnit.JOIN_BUILD, [12000, 4, 12000]),
         ]

q7_p4 = [(OpUnit.SCAN, [1000, 8, 1000]),
         (OpUnit.JOIN_BUILD, [1000, 8, 1000]),
         ]

q7_p5 = [(OpUnit.SCAN, [600000, 28, 600000]),
         (OpUnit.INT_GREATER, [1200000]),
         (OpUnit.JOIN_PROBE, [170000, 4, 14000]),
         (OpUnit.JOIN_PROBE, [14000, 4, 350]),
         (OpUnit.AGG_BUILD, [350, 12, 8]),
         (OpUnit.REAL_MULTIPLY, [350]),
         (OpUnit.REAL_ADD, [700]),
         ]

q7_p6 = [(OpUnit.AGG_PROBE, [4, 12, 4]),
         (OpUnit.SORT_BUILD, [4, 12, 4]),
         ]

q7_p7 = [(OpUnit.SORT_PROBE, [4, 12, 4]),
         ]


q11_p1 = [(OpUnit.SCAN, [25, 14, 25]),
          (OpUnit.REAL_GREATER, [25]),
          (OpUnit.JOIN_BUILD, [1, 4, 1]),
          ]

q11_p2 = [(OpUnit.SCAN, [25, 14, 25]),
          (OpUnit.REAL_GREATER, [25]),
          (OpUnit.JOIN_BUILD, [1, 4, 1]),
          ]

q11_p3 = [(OpUnit.SCAN, [1000, 8, 1000]),
          (OpUnit.JOIN_PROBE, [1000, 4, 50]),
          (OpUnit.JOIN_BUILD, [50, 4, 50]),
          ]

q11_p4 = [(OpUnit.SCAN, [80000, 16, 80000]),
          (OpUnit.JOIN_PROBE, [80000, 4, 4000]),
          (OpUnit.REAL_MULTIPLY, [4000]),
          (OpUnit.REAL_ADD, [4000]),
          ]

q11_p5 = [(OpUnit.SCAN, [80000, 20, 80000]),
          (OpUnit.JOIN_PROBE, [80000, 4, 4000]),
          (OpUnit.REAL_MULTIPLY, [4000]),
          (OpUnit.REAL_ADD, [4000]),
          (OpUnit.AGG_BUILD, [4000, 4, 1000]),
          ]

q11_p6 = [(OpUnit.AGG_PROBE, [4000, 4, 4000]),
          (OpUnit.REAL_MULTIPLY, [4000]),
          (OpUnit.REAL_GREATER, [4000]),
          (OpUnit.SORT_BUILD, [2500, 4, 2500]),
          ]

q11_p7 = [(OpUnit.SORT_PROBE, [2500, 12, 2500]),
          ]


scan_lineitem_p1 = [(OpUnit.SCAN, [600000, 58, 600000]),
                    ]

scan_orders_p1 = [(OpUnit.SCAN, [150000, 4, 150000]),
                  ]

feature_map = {"tpch_q1_p1": q1_p1,
               "tpch_q1_p2": q1_p2,
               "tpch_q1_p3": q1_p3,
               "tpch_q4_p1": q4_p1,
               "tpch_q4_p2": q4_p2,
               "tpch_q4_p3": q4_p3,
               "tpch_q4_p4": q4_p4,
               "tpch_q5_p1": q5_p1,
               "tpch_q5_p2": q5_p1,
               "tpch_q5_p3": q5_p3,
               "tpch_q5_p4": q5_p4,
               "tpch_q5_p5": q5_p5,
               "tpch_q5_p6": q5_p6,
               "tpch_q5_p7": q5_p7,
               "tpch_q5_p8": q5_p8,
               "tpch_q6_p1": q6_p1,
               "tpch_q7_p1": q7_p1,
               "tpch_q7_p2": q7_p1,
               "tpch_q7_p3": q7_p3,
               "tpch_q7_p4": q7_p4,
               "tpch_q7_p5": q7_p5,
               "tpch_q7_p6": q7_p6,
               "tpch_q7_p7": q7_p7,
               "tpch_q11_p1": q11_p1,
               "tpch_q11_p2": q11_p1,
               "tpch_q11_p3": q11_p3,
               "tpch_q11_p4": q11_p4,
               "tpch_q11_p5": q11_p5,
               "tpch_q11_p6": q11_p6,
               "tpch_q11_p7": q11_p7,
               "tpch_scan_lineitem_p1": scan_lineitem_p1,
               "tpch_scan_orders_p1": scan_orders_p1,
               }
