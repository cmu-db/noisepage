//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.h
//
// Identification: src/include/execution/proxy/tile_group_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "storage/tile_group.h"

namespace terrier::execution {

PROXY(TileGroup) {
  DECLARE_MEMBER(0, char[sizeof(storage::TileGroup)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(GetNextTupleSlot);
  DECLARE_METHOD(GetTileGroupId);
};

TYPE_BUILDER(TileGroup, storage::TileGroup);

}  // namespace terrier::execution