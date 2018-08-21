#include "obli.hpp"
#include <iostream>

const u64 c1 = 0xDEADBEAFDEADBEAF;
const u64 c2 = 0xBEAFDEADBEAFDEAD;


int obli_cmov64_test() {
    u64 src64 = c1;
    u64 dst64 = c2;
    u64 res64;

    res64 = obli_cmov64(src64, dst64, true);
    if(res64 != dst64) {
        std::cout << __func__ << ":" << "FAILED:" << std::hex <<  "res:" << res64 
		<< ", src:" << src64 << ", dst:" << dst64 <<  ", cond:" << true << std::endl;
        return -1;
    }

    std::cout << __func__ << ":" << "PASSED" << std::endl;

    res64 = obli_cmov64(src64, dst64, false);
    if(res64 != src64) {
        std::cout << __func__ << ":" << "FAILED:" << std::hex <<  "res:" << res64 
		<< ", src:" << src64 << ", dst:" << dst64 <<  ", cond:" << true << std::endl;
        return -1;
    }

    std::cout << __func__ << ":" << "PASSED" << std::endl;

    return 0;
}

int main() {
    
   obli_cmov64_test();
}
