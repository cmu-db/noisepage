def fix_idx_scan_with_varchar(feature, q_id, p_id, x_loc):
    # Another hack to tweak the feature because we don't model varchar comparisons,
    # so we just add the varchar to the index scan
    if feature == 'IDX_SCAN' and x_loc[3] == 50000 and q_id > 10 and p_id == 2:
        x_loc[1] += 24

    return x_loc
