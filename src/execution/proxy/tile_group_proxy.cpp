//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.cpp
//
// Identification: src/execution/proxy/tile_group_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/tile_group_proxy.h"

namespace terrier::execution {

DEFINE_TYPE(TileGroup, "peloton::storage::TileGroup", opaque);

DEFINE_METHOD(peloton::storage, TileGroup, GetNextTupleSlot);
DEFINE_METHOD(peloton::storage, TileGroup, GetTileGroupId);

}  // namespace terrier::execution
