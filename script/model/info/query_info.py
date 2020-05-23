from type import OpUnit

_Q1_P1 = [(OpUnit.SEQ_SCAN, [600000, 48, 7, 600000]),
          (OpUnit.OP_INTEGER_COMPARE, [600000, 4, 1, 600000]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [600000, 4, 1, 600000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [4800000, 4, 1, 4800000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [1800000, 4, 1, 1800000]),
          (OpUnit.AGG_BUILD, [600000, 32, 2, 4]),
          ]

_Q1_P2 = [(OpUnit.AGG_ITERATE, [4, 92, 10, 4]),
          (OpUnit.SORT_BUILD, [4, 32, 2, 4])
          ]

_Q1_Q3 = [(OpUnit.SORT_ITERATE, [4, 92, 10, 4]),
          (OpUnit.OUTPUT, [4, 92, 10, 4]),
          ]

_Q4_P1 = [(OpUnit.SEQ_SCAN, [150000, 24, 3, 100]),
          (OpUnit.OP_DECIMAL_COMPARE, [300000, 4, 1, 300000]),
          (OpUnit.HASHJOIN_BUILD, [5357, 4, 1, 5357]),
          ]

_Q4_P2 = [(OpUnit.SEQ_SCAN, [600000, 12, 3, 100]),
          (OpUnit.OP_INTEGER_COMPARE, [600000, 4, 1, 600000]),
          (OpUnit.HASHJOIN_PROBE, [366000, 4, 1, 5357]),
          (OpUnit.AGG_BUILD, [5357, 16, 1, 5]),
          (OpUnit.OP_INTEGER_PLUS_OR_MINUS, [5357, 4, 1, 5357]),
          ]

_Q4_P3 = [(OpUnit.AGG_ITERATE, [5, 20, 2,  5]),
          (OpUnit.SORT_BUILD, [5, 16, 1, 5])
          ]

_Q4_P4 = [(OpUnit.SORT_ITERATE, [5, 20, 2, 5]),
          (OpUnit.OUTPUT, [5, 20, 2, 5]),
          ]

_Q5_P1 = [(OpUnit.SEQ_SCAN, [5, 16, 2, 5]),
          (OpUnit.OP_INTEGER_COMPARE, [5, 4, 1, 5]),
          (OpUnit.HASHJOIN_BUILD, [1, 4, 1, 1]),
          ]

_Q5_P2 = [(OpUnit.SEQ_SCAN, [25, 24, 3, 25]),
          (OpUnit.HASHJOIN_PROBE, [25, 4, 1, 5]),
          (OpUnit.HASHJOIN_BUILD, [5, 4, 1, 5]),
          ]

_Q5_P3 = [(OpUnit.SEQ_SCAN, [15000, 8, 2, 15000]),
          (OpUnit.HASHJOIN_PROBE, [15000, 4, 1, 3000]),
          (OpUnit.HASHJOIN_BUILD, [3000, 4, 1, 3000]),
          ]

_Q5_P4 = [(OpUnit.SEQ_SCAN, [150000, 12, 3, 150000]),
          (OpUnit.OP_INTEGER_COMPARE, [300000, 4, 1, 300000]),
          (OpUnit.HASHJOIN_PROBE, [150000, 4, 1, 30000]),
          (OpUnit.HASHJOIN_BUILD, [30000, 4, 1, 30000]),
          ]

_Q5_P5 = [(OpUnit.SEQ_SCAN, [1000, 8, 2, 1000]),
          (OpUnit.HASHJOIN_BUILD, [1000, 8, 2, 1000]),
          ]

_Q5_P6 = [(OpUnit.SEQ_SCAN, [600000, 24, 4, 600000]),
          (OpUnit.HASHJOIN_PROBE, [600000, 4, 1, 120000]),
          (OpUnit.HASHJOIN_PROBE, [120000, 4, 1, 6000]),
          (OpUnit.AGG_BUILD, [6000, 16, 1, 5]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [6000, 4, 1, 6000]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [12000, 4, 1, 12000]),
          ]

_Q5_P7 = [(OpUnit.AGG_ITERATE, [5, 20, 2, 5]),
          (OpUnit.SORT_BUILD, [5, 8, 1, 5]),
          ]

_Q5_P8 = [(OpUnit.SORT_ITERATE, [5, 20, 2, 5]),
          (OpUnit.OUTPUT, [5, 20, 2, 5]),
          ]

_Q6_P1 = [(OpUnit.SEQ_SCAN, [600000, 24, 4, 600000]),
          (OpUnit.OP_DECIMAL_COMPARE, [1800000, 4, 1, 1800000]),
          (OpUnit.OP_INTEGER_COMPARE, [1200000, 4, 1, 1200000]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [4285, 4, 1, 4285]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [4285, 4, 1, 4285]),
          (OpUnit.OUTPUT, [1, 8, 1, 1]),
          ]

_Q7_P1 = [(OpUnit.SEQ_SCAN, [25, 20, 2, 25]),
          (OpUnit.OP_DECIMAL_COMPARE, [50, 4, 1, 50]),
          (OpUnit.SEQ_SCAN, [50, 20, 2, 50]),
          (OpUnit.OP_DECIMAL_COMPARE, [100, 4, 1, 100]),
          (OpUnit.HASHJOIN_BUILD, [4, 4, 1, 2]),
          ]

_Q7_P2 = [(OpUnit.SEQ_SCAN, [15000, 8, 2, 15000]),
          (OpUnit.HASHJOIN_PROBE, [15000, 4, 1, 1200]),
          (OpUnit.HASHJOIN_BUILD, [1200, 4, 1, 1200]),
          ]

_Q7_P3 = [(OpUnit.SEQ_SCAN, [150000, 8, 2, 150000]),
          (OpUnit.HASHJOIN_PROBE, [150000, 4, 1, 12000]),
          (OpUnit.HASHJOIN_BUILD, [12000, 4, 1, 12000]),
          ]

_Q7_P4 = [(OpUnit.SEQ_SCAN, [1000, 8, 2, 1000]),
          (OpUnit.HASHJOIN_BUILD, [1000, 8, 2, 1000]),
          ]

_Q7_P5 = [(OpUnit.SEQ_SCAN, [600000, 28, 5, 600000]),
          (OpUnit.OP_INTEGER_COMPARE, [1200000, 4, 1, 1200000]),
          (OpUnit.HASHJOIN_PROBE, [170000, 4, 1, 14000]),
          (OpUnit.HASHJOIN_PROBE, [14000, 4, 1, 350]),
          (OpUnit.AGG_BUILD, [350, 36, 3, 8]),
          (OpUnit.OP_DECIMAL_MULTIPLY, [350, 4, 1, 350]),
          (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [700, 4, 1, 700]),
          ]

_Q7_P6 = [(OpUnit.AGG_ITERATE, [4, 44, 4, 4]),
          (OpUnit.SORT_BUILD, [4, 36, 3, 4]),
          ]

_Q7_P7 = [(OpUnit.SORT_ITERATE, [4, 44, 4, 4]),
          (OpUnit.OUTPUT, [4, 44, 4, 4]),
          ]


_Q11_P1 = [(OpUnit.SEQ_SCAN, [25, 20, 2, 25]),
           (OpUnit.OP_DECIMAL_COMPARE, [25, 4, 1, 25]),
           (OpUnit.HASHJOIN_BUILD, [1, 4, 1, 1]),
           ]

_Q11_P2 = [(OpUnit.SEQ_SCAN, [1000, 8, 2, 1000]),
           (OpUnit.HASHJOIN_PROBE, [1000, 4, 1, 50]),
           (OpUnit.HASHJOIN_BUILD, [50, 4, 1, 50]),
           ]

_Q11_P3 = [(OpUnit.SEQ_SCAN, [80000, 16, 3, 80000]),
           (OpUnit.HASHJOIN_PROBE, [80000, 4, 1, 4000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [4000, 4, 1, 4000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [4000, 4, 1, 4000]),
           ]

_Q11_P4 = [(OpUnit.SEQ_SCAN, [80000, 20, 4, 80000]),
           (OpUnit.HASHJOIN_PROBE, [80000, 4, 1, 4000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [4000, 4, 1, 4000]),
           (OpUnit.OP_DECIMAL_PLUS_OR_MINUS, [4000, 4, 1, 4000]),
           (OpUnit.AGG_BUILD, [4000, 4, 1, 4000]),
           ]

_Q11_P5 = [(OpUnit.AGG_ITERATE, [4000, 12, 2, 4000]),
           (OpUnit.OP_DECIMAL_MULTIPLY, [4000, 4, 1, 4000]),
           (OpUnit.OP_DECIMAL_COMPARE, [4000, 4, 1, 4000]),
           (OpUnit.SORT_BUILD, [2500, 8, 1, 2500]),
           ]

_Q11_P6 = [(OpUnit.SORT_ITERATE, [2500, 12, 2, 2500]),
           (OpUnit.OUTPUT, [2500, 12, 2, 2500]),
           ]


_SCAN_LINEITEM_P1 = [(OpUnit.SEQ_SCAN, [600000, 64, 10, 600000]),
                     (OpUnit.OUTPUT, [600000, 64, 10, 600000])
                     ]

_SCAN_ORDERS_P1 = [(OpUnit.SEQ_SCAN, [150000, 4, 1, 150000]),
                   (OpUnit.OUTPUT, [150000, 4, 1, 150000])
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

# Map from query pipeline identifier to the memory adjustment factor
MEM_ADJUST_MAP = {
               "tpch_q1_p1": 1.2105,
               "tpch_q1_p2": 1.375,
               "tpch_q4_p1": 1.75,
               "tpch_q4_p2": 0.5455,
               "tpch_q4_p3": 0.625,
               "tpch_q5_p1": 1.0,
               "tpch_q5_p2": 1.75,
               "tpch_q5_p3": 2.25,
               "tpch_q5_p4": 2.25,
               "tpch_q5_p5": 1.0,
               "tpch_q5_p6": 0.6364,
               "tpch_q5_p7": 1.25,
               "tpch_q7_p1": 3.0,
               "tpch_q7_p2": 3.0,
               "tpch_q7_p3": 3.0,
               "tpch_q7_p4": 1.0,
               "tpch_q7_p5": 0.5714,
               "tpch_q7_p6": 0.5556,
               "tpch_q11_p1": 1.0,
               "tpch_q11_p2": 1.0,
               "tpch_q11_p4": 1.2,
               "tpch_q11_p5": 1,
               }
