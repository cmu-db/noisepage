from type import OpUnit

_Q1_P1 = [(OpUnit.SEQ_SCAN, [60000000, 48, 7, 60000000]),
          (OpUnit.OP_INTEGER_COMPARE, [60000000, 4, 1, 60000000]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [60000000, 4, 1, 60000000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [480000000, 4, 1, 480000000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [180000000, 4, 1, 180000000]),
          (OpUnit.AGG_BUILD, [60000000, 32, 2, 4]),
          ]

_Q1_P2 = [(OpUnit.AGG_ITERATE, [4, 92, 10, 4]),
          (OpUnit.SORT_BUILD, [4, 32, 2, 4])
          ]

_Q1_Q3 = [(OpUnit.SORT_ITERATE, [4, 92, 10, 4]),
          (OpUnit.OUTPUT, [4, 92, 10, 0]),
          ]

_Q4_P1 = [(OpUnit.SEQ_SCAN, [15000000, 24, 3, 15000000]),
          (OpUnit.OP_DECIMAL_COMPARE, [30000000, 4, 1, 30000000]),
          (OpUnit.HASHJOIN_BUILD, [535700, 4, 1, 535700]),
          ]

_Q4_P2 = [(OpUnit.SEQ_SCAN, [60000000, 12, 3, 60000000]),
          (OpUnit.OP_INTEGER_COMPARE, [60000000, 4, 1, 60000000]),
          (OpUnit.HASHJOIN_PROBE, [37000000, 4, 1, 535700]),
          (OpUnit.AGG_BUILD, [535700, 16, 1, 5]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [535700, 4, 1, 535700]),
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

_Q5_P3 = [(OpUnit.SEQ_SCAN, [1500000, 8, 2, 1500000]),
          (OpUnit.HASHJOIN_PROBE, [1500000, 4, 1, 300000]),
          (OpUnit.HASHJOIN_BUILD, [300000, 4, 1, 300000]),
          ]

_Q5_P4 = [(OpUnit.SEQ_SCAN, [15000000, 12, 3, 15000000]),
          (OpUnit.OP_INTEGER_COMPARE, [30000000, 4, 1, 30000000]),
          (OpUnit.HASHJOIN_PROBE, [15000000, 4, 1, 3000000]),
          (OpUnit.HASHJOIN_BUILD, [3000000, 4, 1, 3000000]),
          ]

_Q5_P5 = [(OpUnit.SEQ_SCAN, [100000, 8, 2, 100000]),
          (OpUnit.HASHJOIN_BUILD, [100000, 8, 2, 100000]),
          ]

_Q5_P6 = [(OpUnit.SEQ_SCAN, [60000000, 24, 4, 60000000]),
          (OpUnit.HASHJOIN_PROBE, [60000000, 4, 1, 12000000]),
          (OpUnit.HASHJOIN_PROBE, [12000000, 4, 1, 500000]),
          (OpUnit.AGG_BUILD, [500000, 16, 1, 5]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [500000, 4, 1, 500000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [1000000, 4, 1, 1000000]),
          ]

_Q5_P7 = [(OpUnit.AGG_ITERATE, [5, 20, 2, 5]),
          (OpUnit.SORT_BUILD, [5, 8, 1, 5]),
          ]

_Q5_P8 = [(OpUnit.SORT_ITERATE, [5, 20, 2, 5]),
          (OpUnit.OUTPUT, [5, 20, 2, 0]),
          ]

_Q6_P1 = [(OpUnit.SEQ_SCAN, [60000000, 24, 4, 60000000]),
          (OpUnit.OP_DECIMAL_COMPARE, [180000000, 4, 1, 180000000]),
          (OpUnit.OP_INTEGER_COMPARE, [120000000, 4, 1, 120000000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [428500, 4, 1, 428500]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [428500, 4, 1, 428500]),
          (OpUnit.OUTPUT, [1, 8, 1, 0]),
          ]

_Q7_P1 = [(OpUnit.SEQ_SCAN, [25, 20, 2, 25]),
          (OpUnit.OP_DECIMAL_COMPARE, [50, 4, 1, 50]),
          (OpUnit.SEQ_SCAN, [50, 20, 2, 50]),
          (OpUnit.OP_DECIMAL_COMPARE, [100, 4, 1, 100]),
          (OpUnit.HASHJOIN_BUILD, [4, 4, 1, 2]),
          ]

_Q7_P2 = [(OpUnit.SEQ_SCAN, [1500000, 8, 2, 1500000]),
          (OpUnit.HASHJOIN_PROBE, [1500000, 4, 1, 120000]),
          (OpUnit.HASHJOIN_BUILD, [120000, 4, 1, 120000]),
          ]

_Q7_P3 = [(OpUnit.SEQ_SCAN, [15000000, 8, 2, 15000000]),
          (OpUnit.HASHJOIN_PROBE, [15000000, 4, 1, 1200000]),
          (OpUnit.HASHJOIN_BUILD, [1200000, 4, 1, 1200000]),
          ]

_Q7_P4 = [(OpUnit.SEQ_SCAN, [100000, 8, 2, 100000]),
          (OpUnit.HASHJOIN_BUILD, [100000, 8, 2, 100000]),
          ]

_Q7_P5 = [(OpUnit.SEQ_SCAN, [60000000, 28, 5, 60000000]),
          (OpUnit.OP_INTEGER_COMPARE, [120000000, 4, 1, 120000000]),
          (OpUnit.HASHJOIN_PROBE, [17000000, 4, 1, 1400000]),
          (OpUnit.HASHJOIN_PROBE, [1400000, 4, 1, 35000]),
          (OpUnit.AGG_BUILD, [35000, 36, 3, 8]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [35000, 4, 1, 35000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [70000, 4, 1, 70000]),
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

_Q11_P2 = [(OpUnit.SEQ_SCAN, [100000, 8, 2, 100000]),
           (OpUnit.HASHJOIN_PROBE, [100000, 4, 1, 5000]),
           (OpUnit.HASHJOIN_BUILD, [5000, 4, 1, 5000]),
           ]

_Q11_P3 = [(OpUnit.SEQ_SCAN, [8000000, 16, 3, 8000000]),
           (OpUnit.HASHJOIN_PROBE, [8000000, 4, 1, 400000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [400000, 4, 1, 400000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [400000, 4, 1, 400000]),
           ]

_Q11_P4 = [(OpUnit.SEQ_SCAN, [8000000, 20, 4, 8000000]),
           (OpUnit.HASHJOIN_PROBE, [8000000, 4, 1, 300000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [300000, 4, 1, 300000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [300000, 4, 1, 300000]),
           (OpUnit.AGG_BUILD, [300000, 4, 1, 300000]),
           ]

_Q11_P5 = [(OpUnit.AGG_ITERATE, [300000, 12, 2, 300000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [300000, 4, 1, 300000]),
           (OpUnit.OP_DECIMAL_COMPARE, [300000, 4, 1, 300000]),
           (OpUnit.SORT_BUILD, [1, 8, 1, 1]),
           ]

_Q11_P6 = [(OpUnit.SORT_ITERATE, [1, 12, 2, 1]),
           (OpUnit.OUTPUT, [1, 12, 2, 0]),
           ]


_SCAN_LINEITEM_P1 = [(OpUnit.SEQ_SCAN, [60000000, 64, 10, 60000000]),
                     (OpUnit.OUTPUT, [60000000, 64, 10, 0])
                     ]

_SCAN_ORDERS_P1 = [(OpUnit.SEQ_SCAN, [15000000, 4, 1, 15000000]),
                   (OpUnit.OUTPUT, [15000000, 4, 1, 0])
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
