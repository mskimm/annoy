#include "../../src/annoylib.h"
#include "../../src/kissrandom.h"
#include "../../src/experimental/annoylib.h"

int main(int argc, char **argv) {

  experimental::AnnoyIndex<int, double, Angular, Kiss32Random, experimental::MemoryStorage> t =
      experimental::AnnoyIndex<int, double, Angular, Kiss32Random, experimental::MemoryStorage>(10, NULL);

  return EXIT_SUCCESS;

}
