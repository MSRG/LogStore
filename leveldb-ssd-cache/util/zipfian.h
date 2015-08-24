#ifndef ZIPFIAN_H_
#define ZIPFIAN_H_

#include <math.h>
#include <iostream>
//#include "util/testharness.h"

namespace leveldb {

/* This class was borrowed from the YCSB benchmark suite.  The code is available
 * on GitHub at https://github.com/brianfrankcooper/YCSB.  We use it here as an 
 * alternative to LevelDB's own skewed random number generator.
 */
class ZipfianGenerator {     
 public:
  constexpr static const double ZIPFIAN_CONSTANT = 0.9;

 private: 
  Random& random_;
  long items_;
  long base_;
  double zipfianconstant_;
  double alpha_,zetan_,eta_,theta_,zeta2theta_;
  long countforzeta_;
  bool allowitemcountdecrease_ = false;

 public:
  ZipfianGenerator(Random& r, long min, long max, double zipfianconstant, double zetan)
    : random_(r),
      items_(max - min + 1), 
      base_(min), 
      zipfianconstant_(zipfianconstant),
      theta_(zipfianconstant), 
      zetan_(zetan)
  {
    zeta2theta_ = zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    countforzeta_ = items_;
    eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_/zetan_);
    NextInt();
  }
  ZipfianGenerator(Random& r, long min, long max, double zipfianconstant)
    : ZipfianGenerator(r, min, max, zipfianconstant, zetastatic(max - min + 1, zipfianconstant)) 
  {}
	ZipfianGenerator(Random& r, long min, long max)
    : ZipfianGenerator(r, min, max, ZIPFIAN_CONSTANT) 
  {}
	ZipfianGenerator(Random& r, long items): ZipfianGenerator(r, 0, items - 1) {}

  int NextInt(int itemcount) {
    return static_cast<int>(NextLong(itemcount));
  }

  long NextLong(long itemcount) {
    if (itemcount != countforzeta_) {
      if (itemcount > countforzeta_) {
        zetan_ = zeta(countforzeta_, itemcount, theta_, zetan_);
        eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
      } else if ( (itemcount < countforzeta_) && (allowitemcountdecrease_) ) {
        zetan_ = zeta(itemcount, theta_);
        eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
      }
    }

    //double u = (double) random_.Uniform(1000000) / 1000000.0; //Utils.random().nextDouble();
    //double u = (double) random_.Uniform(500000000) / 500000000.0; //Utils.random().nextDouble();
    double u = (static_cast<double>(rand()) / static_cast<double>(RAND_MAX));
    double uz = u * zetan_;
    
    if (uz < 1.0) {
      return 0;
    }

    if (uz < 1.0 + pow(0.5, theta_))  {
      return 1;
    }

    long ret = base_ + (long)((itemcount) * pow(eta_ * u - eta_ + 1, alpha_));
    return ret;
  }

  int NextInt()  {
    return static_cast<int>(NextLong(items_));
  }

  long NextLong() {
    return NextLong(items_);
  }

 private:
  static double zetastatic(long st, long n, double theta, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < n; i++) {
      sum += 1 / (pow(i+1, theta));
    }
    return sum;
  }

  static double zetastatic(long n, double theta) {
    return zetastatic(0,n,theta,0);
  }
	
  double zeta(long st, long n, double theta, double initialsum) {
    countforzeta_ = n;
    return zetastatic(st, n, theta, initialsum);
  }

  double zeta(long n, double theta) {
    countforzeta_ = n;
    return zetastatic(n, theta);
  }
};

} // namespace leveldb

#endif // ZIPFIAN_H_
