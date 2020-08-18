def transformFeature(feature, q_id, p_id, x_loc):
    # Hack to tweak the feature based on the query
    if q_id == 29 and p_id == 2:
        # SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1    AND NO_W_ID = $2
        # ORDER BY NO_O_ID ASC  LIMIT 1
        #
        # q31 returns ~850 tuples from IDX_SCAN to SORT_BUILD
        if feature == 'SORT_BUILD':
            # Set # input rows to SORT_BUILD as 850
            # Set cardinality to 1 since query runs with LIMIT 1
            x_loc[0] = 850
            x_loc[3] = 1
        elif feature == 'IDX_SCAN':
            # Set # output rows of IDX_SCAN as 850
            x_loc[3] = 850
    elif q_id == 39 and p_id == 2:
        # SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT  FROM ORDER_LINE, STOCK WHERE
        # OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID < $3 AND OL_O_ID >= $4 AND S_W_ID = $5
        # AND S_I_ID = OL_I_ID AND S_QUANTITY < $6
        if feature == 'AGG_BUILD':
            # Set agg_build input rows to 200, assume output unchanged
            # Since there's distinct, set the correct key size/input key
            x_loc[0] = 200
            x_loc[1] = 4
            x_loc[2] = 1
            x_loc[3] = 1
        elif feature == 'IDX_SCAN' and x_loc[2] == 2:
            # Scale to account for the "loop" factor
            # Then don't need to adjust the prediction metric
            x_loc[5] = 200
        elif feature == 'IDX_SCAN' and x_loc[2] == 3:
            # Outer index scan returns multiple values
            x_loc[3] = 200

    return x_loc

