from type import OpUnit

_Q1_P1 = [(OpUnit.SEQ_SCAN, [6000000, 48, 7, 6000000]),
          (OpUnit.OP_INTEGER_COMPARE, [6000000, 4, 1, 6000000]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [6000000, 4, 1, 6000000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [48000000, 4, 1, 48000000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [18000000, 4, 1, 18000000]),
          (OpUnit.AGG_BUILD, [6000000, 32, 2, 4]),
          ]

_Q1_P2 = [(OpUnit.AGG_ITERATE, [4, 92, 10, 4]),
          (OpUnit.SORT_BUILD, [4, 32, 2, 4])
          ]

_Q1_Q3 = [(OpUnit.SORT_ITERATE, [4, 92, 10, 4]),
          (OpUnit.OUTPUT, [4, 92, 10, 0]),
          ]

_Q4_P1 = [(OpUnit.SEQ_SCAN, [1500000, 24, 3, 1500000]),
          (OpUnit.OP_DECIMAL_COMPARE, [3000000, 4, 1, 3000000]),
          (OpUnit.HASHJOIN_BUILD, [53570, 4, 1, 53570]),
          ]

_Q4_P2 = [(OpUnit.SEQ_SCAN, [6000000, 12, 3, 6000000]),
          (OpUnit.OP_INTEGER_COMPARE, [6000000, 4, 1, 6000000]),
          (OpUnit.HASHJOIN_PROBE, [3700000, 4, 1, 53570]),
          (OpUnit.AGG_BUILD, [53570, 16, 1, 5]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [53570, 4, 1, 53570]),
          ]

_Q4_P3 = [(OpUnit.AGG_ITERATE, [5, 20, 2,  5]),
          (OpUnit.SORT_BUILD, [5, 16, 1, 5])
          ]

_Q4_P4 = [(OpUnit.SORT_ITERATE, [5, 20, 2, 5]),
          (OpUnit.OUTPUT, [5, 20, 2, 0]),
          ]

_Q5_P1 = [(OpUnit.SEQ_SCAN, [5, 16, 2, 5]),
          (OpUnit.OP_INTEGER_COMPARE, [5, 4, 1, 5]),
          (OpUnit.HASHJOIN_BUILD, [1, 4, 1, 1]),
          ]

_Q5_P2 = [(OpUnit.SEQ_SCAN, [25, 24, 3, 25]),
          (OpUnit.HASHJOIN_PROBE, [25, 4, 1, 5]),
          (OpUnit.HASHJOIN_BUILD, [5, 4, 1, 5]),
          ]

_Q5_P3 = [(OpUnit.SEQ_SCAN, [150000, 8, 2, 150000]),
          (OpUnit.HASHJOIN_PROBE, [150000, 4, 1, 30000]),
          (OpUnit.HASHJOIN_BUILD, [30000, 4, 1, 30000]),
          ]

_Q5_P4 = [(OpUnit.SEQ_SCAN, [1500000, 12, 3, 1500000]),
          (OpUnit.OP_INTEGER_COMPARE, [3000000, 4, 1, 3000000]),
          (OpUnit.HASHJOIN_PROBE, [1500000, 4, 1, 300000]),
          (OpUnit.HASHJOIN_BUILD, [300000, 4, 1, 300000]),
          ]

_Q5_P5 = [(OpUnit.SEQ_SCAN, [10000, 8, 2, 10000]),
          (OpUnit.HASHJOIN_BUILD, [10000, 8, 2, 10000]),
          ]

_Q5_P6 = [(OpUnit.SEQ_SCAN, [6000000, 24, 4, 6000000]),
          (OpUnit.HASHJOIN_PROBE, [6000000, 4, 1, 1200000]),
          (OpUnit.HASHJOIN_PROBE, [1200000, 4, 1, 50000]),
          (OpUnit.AGG_BUILD, [50000, 16, 1, 5]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [50000, 4, 1, 50000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [100000, 4, 1, 100000]),
          ]

_Q5_P7 = [(OpUnit.AGG_ITERATE, [5, 20, 2, 5]),
          (OpUnit.SORT_BUILD, [5, 8, 1, 5]),
          ]

_Q5_P8 = [(OpUnit.SORT_ITERATE, [5, 20, 2, 5]),
          (OpUnit.OUTPUT, [5, 20, 2, 0]),
          ]

_Q6_P1 = [(OpUnit.SEQ_SCAN, [6000000, 24, 4, 6000000]),
          (OpUnit.OP_DECIMAL_COMPARE, [18000000, 4, 1, 18000000]),
          (OpUnit.OP_INTEGER_COMPARE, [12000000, 4, 1, 12000000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [42850, 4, 1, 42850]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [42850, 4, 1, 42850]),
          (OpUnit.OUTPUT, [1, 8, 1, 0]),
          ]

_Q7_P1 = [(OpUnit.SEQ_SCAN, [25, 20, 2, 25]),
          (OpUnit.OP_DECIMAL_COMPARE, [50, 4, 1, 50]),
          (OpUnit.SEQ_SCAN, [50, 20, 2, 50]),
          (OpUnit.OP_DECIMAL_COMPARE, [100, 4, 1, 100]),
          (OpUnit.HASHJOIN_BUILD, [4, 4, 1, 2]),
          ]

_Q7_P2 = [(OpUnit.SEQ_SCAN, [150000, 8, 2, 150000]),
          (OpUnit.HASHJOIN_PROBE, [150000, 4, 1, 12000]),
          (OpUnit.HASHJOIN_BUILD, [12000, 4, 1, 12000]),
          ]

_Q7_P3 = [(OpUnit.SEQ_SCAN, [1500000, 8, 2, 1500000]),
          (OpUnit.HASHJOIN_PROBE, [1500000, 4, 1, 120000]),
          (OpUnit.HASHJOIN_BUILD, [120000, 4, 1, 120000]),
          ]

_Q7_P4 = [(OpUnit.SEQ_SCAN, [10000, 8, 2, 10000]),
          (OpUnit.HASHJOIN_BUILD, [10000, 8, 2, 10000]),
          ]

_Q7_P5 = [(OpUnit.SEQ_SCAN, [6000000, 28, 5, 6000000]),
          (OpUnit.OP_INTEGER_COMPARE, [12000000, 4, 1, 12000000]),
          (OpUnit.HASHJOIN_PROBE, [1700000, 4, 1, 140000]),
          (OpUnit.HASHJOIN_PROBE, [140000, 4, 1, 3500]),
          (OpUnit.AGG_BUILD, [3500, 36, 3, 8]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [3500, 4, 1, 3500]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [7000, 4, 1, 7000]),
          ]

_Q7_P6 = [(OpUnit.AGG_ITERATE, [4, 44, 4, 4]),
          (OpUnit.SORT_BUILD, [4, 36, 3, 4]),
          ]

_Q7_P7 = [(OpUnit.SORT_ITERATE, [4, 44, 4, 4]),
          (OpUnit.OUTPUT, [4, 44, 4, 0]),
          ]


_Q11_P1 = [(OpUnit.SEQ_SCAN, [25, 20, 2, 25]),
           (OpUnit.OP_DECIMAL_COMPARE, [25, 4, 1, 25]),
           (OpUnit.HASHJOIN_BUILD, [1, 4, 1, 1]),
           ]

_Q11_P2 = [(OpUnit.SEQ_SCAN, [10000, 8, 2, 10000]),
           (OpUnit.HASHJOIN_PROBE, [10000, 4, 1, 500]),
           (OpUnit.HASHJOIN_BUILD, [500, 4, 1, 500]),
           ]

_Q11_P3 = [(OpUnit.SEQ_SCAN, [800000, 16, 3, 800000]),
           (OpUnit.HASHJOIN_PROBE, [800000, 4, 1, 40000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [40000, 4, 1, 40000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [40000, 4, 1, 40000]),
           ]

_Q11_P4 = [(OpUnit.SEQ_SCAN, [800000, 20, 4, 800000]),
           (OpUnit.HASHJOIN_PROBE, [800000, 4, 1, 30000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [30000, 4, 1, 30000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [30000, 4, 1, 30000]),
           (OpUnit.AGG_BUILD, [30000, 4, 1, 30000]),
           ]

_Q11_P5 = [(OpUnit.AGG_ITERATE, [30000, 12, 2, 30000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [30000, 4, 1, 30000]),
           (OpUnit.OP_DECIMAL_COMPARE, [30000, 4, 1, 30000]),
           (OpUnit.SORT_BUILD, [2000, 8, 1, 2000]),
           ]

_Q11_P6 = [(OpUnit.SORT_ITERATE, [2000, 12, 2, 2000]),
           (OpUnit.OUTPUT, [2000, 12, 2, 0]),
           ]


_SCAN_LINEITEM_P1 = [(OpUnit.SEQ_SCAN, [6000000, 64, 10, 6000000]),
                     (OpUnit.OUTPUT, [6000000, 64, 10, 0])
                     ]

_SCAN_ORDERS_P1 = [(OpUnit.SEQ_SCAN, [1500000, 4, 1, 1500000]),
                   (OpUnit.OUTPUT, [1500000, 4, 1, 0])
                   ]

# Map from the query pipeline identifier to the their opunit features
FEATURE_MAP = {"tpch_q1_p1": _Q1_P1,
               "tpch_q1_p2": _Q1_P2,
               "tpch_q1_p3": _Q1_Q3,
               "tpch_q4_p1": _Q4_P1,
               "tpch_q4_p2": _Q4_P2,
               "tpch_q4_p3": _Q4_P3,
               "tpch_q4_p4": _Q4_P4,
               "tpch_q5_p1": _Q5_P1,
               "tpch_q5_p2": _Q5_P2,
               "tpch_q5_p3": _Q5_P3,
               "tpch_q5_p4": _Q5_P4,
               "tpch_q5_p5": _Q5_P5,
               "tpch_q5_p6": _Q5_P6,
               "tpch_q5_p7": _Q5_P7,
               "tpch_q5_p8": _Q5_P8,
               "tpch_q6_p1": _Q6_P1,
               "tpch_q7_p1": _Q7_P1,
               "tpch_q7_p2": _Q7_P2,
               "tpch_q7_p3": _Q7_P3,
               "tpch_q7_p4": _Q7_P4,
               "tpch_q7_p5": _Q7_P5,
               "tpch_q7_p6": _Q7_P6,
               "tpch_q7_p7": _Q7_P7,
               "tpch_q11_p1": _Q11_P1,
               "tpch_q11_p2": _Q11_P2,
               "tpch_q11_p3": _Q11_P3,
               "tpch_q11_p4": _Q11_P4,
               "tpch_q11_p5": _Q11_P5,
               "tpch_q11_p6": _Q11_P6,
               "tpch_scan_lineitem_p1": _SCAN_LINEITEM_P1,
               "tpch_scan_orders_p1": _SCAN_ORDERS_P1,
               }
