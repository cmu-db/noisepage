// Should return 1

fun main() -> int32 {
  if (true == true) {
    if (false < true) {
      if (true > false) {
        if (1.0 == 1.0) {
          if (1.0 <= 1.0) {
            if (2.0 > 1.0) {
              if (2.0 >= 1.0) {
                if (2.0 != 1.0) {
                  if (1 < 2) {
                    if (1 <= 2) {
                      if (2 > 1) {
                        if (2 >= 1) {
                          if (2 != 1) {
                            return 1
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return 0
}
