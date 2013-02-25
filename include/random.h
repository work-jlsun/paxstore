#ifndef PAXSTORE_RANDOM__
#define PAXSTORE_RANDOM__

#include <stdint.h>
#include "slice.h"
#include "kvproposal.h"

class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) { }
  uint32_t Next() ;
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

//Slice CompressibleString(Random* rnd, double compressed_fraction,int len, std::string* dst);
//Slice RandomString(Random* rnd, int len, std::string* dst) ;

class RandomGenerator {
 private:
  std::string data_;
  int pos_;
 public:
  RandomGenerator() ;
  Slice Generate(int len) ;
};

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  U64 bytes_;
  double last_op_finish_;
 public:
  Stats() {
    next_report_ = 100;
    last_op_finish_ = 0;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = 0;
    finish_ = 0;
  }

void Start() ;	
void cls();
void Stop() ;
void Setdone(int d){
	done_ = d;
}

void AddBytes(U64 n) {
	bytes_ += n;
}

void Report() ;
};
#endif
