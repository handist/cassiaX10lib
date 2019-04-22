#ifndef CASSIA_AFFINITY_H
#define CASSIA_AFFINITY_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <pthread.h>
#include <iostream>

#include <x10/lang/Rail.h>

class Cassia {
public:
    static void promote(const int cpu) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(cpu, &mask);
        if (0 != pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask)) {
            std::cerr << "invalid cpu id (value = " << cpu << ")" << std::endl;
            abort();
        }
    }

    static void promote(const x10::lang::Rail<int> * cpus) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        for (int i = 0; i < cpus->FMGL(size); i++) {
            CPU_SET(cpus->raw[i], &mask);
        }
        if (0 != pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask)) {
            std::cerr << "invalid cpu id (value = " << cpus->raw << ")" << std::endl;
            abort();
        }
    }

    static void promote(const int min, const int max) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        for (int i = min; i <= max; i++) {
            CPU_SET(i, &mask);
        }
        if (0 != pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask)) {
            std::cerr << "invalid cpu id (value = " << min << "-" << max << ")" << std::endl;
            abort();
        }
    }
};

#endif // CASSIA_AFFINITY_H
