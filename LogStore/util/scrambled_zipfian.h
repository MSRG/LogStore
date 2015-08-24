#ifndef SCRAMBLED_ZIPFIAN_H_
#define SCRAMBLED_ZIPFIAN_H_

#include "util/zipfian.h"
#include "util/hash.h"

namespace leveldb {

class ScrambledZipfianGenerator {
	constexpr static double ZETAN = 26.46902820178302;
  constexpr static double USED_ZIPFIAN_CONSTANT = 0.99;

 private:
	ZipfianGenerator* gen_;
	long min_;
  long max_;
  long item_count_;

 public:
	ScrambledZipfianGenerator(Random& r, long items)
    : ScrambledZipfianGenerator(r, 0, items-1) { }

	ScrambledZipfianGenerator(Random& r, long min, long max)
    : ScrambledZipfianGenerator(r, min, max, ZipfianGenerator::ZIPFIAN_CONSTANT)
  {}

  ScrambledZipfianGenerator(Random& r, long min, long max, double zipfianconstant)
    : min_(min), max_(max), item_count_(max-min+1) {
		if (zipfianconstant == USED_ZIPFIAN_CONSTANT) {
		    gen_ = new ZipfianGenerator(r, 0, item_count_, zipfianconstant, ZETAN);
		} else {
		    gen_ = new ZipfianGenerator(r, 0, item_count_, zipfianconstant);
		}
	}

  ~ScrambledZipfianGenerator() {
    if (gen_) {
      delete gen_;
    }
  }

	int NextInt() {
		return static_cast<int>(NextLong());
	}

	long NextLong() {
		long ret = gen_->NextLong();
		ret = min_ + hash64(ret) % item_count_;
		return ret;
	}

 private:
  constexpr static long FNV_offset_basis_64 = 0xCBF29CE484222325;
  constexpr static long FNV_prime_64 = 1099511628211;
  static long hash64(long val) {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
	  long hashval = FNV_offset_basis_64;
    for (int i = 0; i < 8; i++) {
	    long octet = val & 0x00ff;
	    val = val >> 8;
	    
	    hashval = hashval ^ octet;
	    hashval = hashval * FNV_prime_64;
    }
    return abs(hashval);
  }
};

}  // namespace leveldb

#endif // SCRAMBLED_ZIPFIAN_H_
