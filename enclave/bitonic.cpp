

#define _GNU_SOURCE
#include <sched.h>
#include <sys/mman.h>
#include <stdatomic.h>
#include <sys/sysinfo.h>
struct thread_group{
    int * pid;
    int len;
};
_Atomic int index, done;
auto cur_mergesort = 1;
auto counter = 0;
void obli_swap(int* i, int* j){
    int res = 0, res1 = (*i)^(*j);
 asm volatile("cmp %1, %0\n\tcmovl %1, %0": "=r"(res): "r"(*i), "0"(*j));
 res1 = res1^res;
 *i = res;
 *j = res1;
}
#include <stdio.h>
template<typename T>
struct params{
T * q; int pow; int tid;
} ;
params<int> xp[16];
template<typename T>
int thread_fn(void * param){
    auto q = ((params<T>*)param)->q;
    auto pow = ((params<T>*)param)->pow;
    auto tid = ((params<T>*)param)->tid;
    auto ren = (pow*(pow+1))>>1;
    auto tot = (1<<(pow-1));
    while(1){
        int cur = index;
        bool iszer = false;
        
        do{
            
            if(cur>=ren*tot) return 0;
            if(cur%tot==0&&cur!=0){
                if(tid!=0){
                    cur=-1;
                    continue;
                }
                int x = done;
                while(x<cur) x = done;
                counter++;
                if(counter==cur_mergesort){ counter-=cur_mergesort;cur_mergesort++;}
                
            }
        }while(!atomic_compare_exchange_weak(&index, &cur, cur+1));
        auto dist = 1<<(cur_mergesort-counter-1);
        auto i0 = ((cur % tot)%dist)+(2*dist*((cur%tot)/dist));
        if(counter==0){
            obli_swap(&q[i0], &q[i0+(dist-((cur % tot)%dist))]);
        }
        else{
            
            obli_swap(&q[i0], &q[i0+dist]);

        }

        atomic_fetch_add(&done, 1);

    }
    return 0;
}
void * x(void * ards){
    printf("HI");
    return 0;
}
#include <pthread.h>
int xx[]={5,23,7,32,1,2,6,4,32,7,5,234,6,3,12,5,76,5,3,5,3,4,6,32,7,8,3,4,5,23,7,32,1,2,6,4,32,7,5,234,6,3,12,5,76,5,3,5,3,4,6,32,7,8,3,4};
char addrs[16][1024*1024];
void launch_threads(){
    auto cores = get_nprocs();
    pthread_t threads[16];
    for(auto i = 0;i<cores;i++){
        xp[i].q = xx;
        xp[i].pow = 5;
        xp[i].tid = i;
        pthread_create(&threads[i],0,(void *(*)(void *))thread_fn<int>,  (void*)&xp[i]);
    }
}
#include <unistd.h>

int main(){
    launch_threads();
    sleep(2);
    for(auto i = 0;i<16;i++){
        printf("%d,", xx[i]);
    }
}
