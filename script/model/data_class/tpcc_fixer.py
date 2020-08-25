# SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1    AND NO_W_ID = $2
# ORDER BY NO_O_ID ASC  LIMIT 1
select_limit_1 = 29

# SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT  FROM ORDER_LINE, STOCK WHERE
# OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID < $3 AND OL_O_ID >= $4 AND S_W_ID = $5
# AND S_I_ID = OL_I_ID AND S_QUANTITY < $6
idx_join_query = 39

# UPDATE ORDER_LINE SET OL_DELIVERY_D = $1 WHERE OL_O_ID = $2
# AND OL_D_ID = $3 AND OL_W_ID = $4
update_orderline = 33

# SELECT SUM(OL_AMOUNT) AS OL_TOTAL FROM ORDER_LINE WHERE OL_O_ID = $1
# AND OL_D_ID = $2 AND OL_W_ID = $3
keyless_aggregate = 34


def transform_feature(feature, q_id, p_id, x_loc):
    # Hack to tweak the feature based on the query
    if q_id == select_limit_1 and p_id == 2:
        if feature == 'SORT_BUILD':
            # Set # input rows to SORT_BUILD as 850
            # Set cardinality to 1 since query runs with LIMIT 1
            x_loc[0] = 850
            x_loc[3] = 1
        elif feature == 'IDX_SCAN':
            # Set # output rows of IDX_SCAN as 850
            x_loc[3] = 850
    elif q_id == idx_join_query and p_id == 2:
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
    elif q_id == update_orderline and p_id == 1:
        # Query processes 10 tuples, so idx_scan looks up 10
        if feature == 'IDX_SCAN':
            x_loc[3] = 10
        # Update then updates 10
        elif feature == 'UPDATE':
            x_loc[0] = 10
            x_loc[3] = 10
    elif q_id == keyless_aggregate and p_id == 2:
        # Query processes 10 tuples, so IDX_SCAN looks up 10
        if feature == 'IDX_SCAN':
            x_loc[3] = 10
        # AGG_BUILD has 10 tuples as an input.
        # Don't know the cardinality but keyless agg so doesn't matter
        elif feature == 'AGG_BUILD':
            x_loc[0] = 10

    return x_loc

