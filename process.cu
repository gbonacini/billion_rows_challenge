// -----------------------------------------------------------------
// process - A billion rows challenge related processor
// Copyright (C) 2024 Gabriele Bonacini
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 3 of the License, or
// (at your option) any later version.
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software Foundation,
// Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA
// -----------------------------------------------------------------

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <linux/mman.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <utility>
#include <thread>

using std::cerr,
      std::string, 
      std::unordered_map, 
      std::vector, 
      std::unordered_set, 
      std::sort,
      std::pair,
      std::get,
      std::stoi,
      std::filesystem::is_regular_file,
      std::thread,
      std::ofstream,
      std::chrono::high_resolution_clock,
      std::chrono::duration_cast,
      std::chrono::duration,
      std::chrono::milliseconds;

__global__ void process(const uint16_t*   stats, const int32_t*  tempers, size_t rows, 
                        int32_t*          max,   int32_t*        min,     unsigned long long* sum,
                        uint32_t*         count){

   unsigned int cidx { blockIdx.x * blockDim.x + threadIdx.x },
                cblk { blockDim.x * gridDim.x };

   for(size_t idx{cidx}; idx<rows; idx += cblk){
      size_t outIdx = stats[idx];
      atomicMax(max + outIdx, tempers[idx]);
      atomicMin(min + outIdx, tempers[idx]);
      atomicAdd(count + outIdx, 1);
      atomicAdd(sum + outIdx, (unsigned long long) tempers[idx]);
   }
}

int main(int argc, char** argv){

   auto printError { [&](const char* msg){ cerr << msg << '\n' << "Syntax:\n" << argv[0] << " <filename>\n"; 
                                           exit(1); 
                                         } 
                   };

   if(argc != 2)                     printError("Invalid argument list");
   if(! is_regular_file(argv[1]))    printError("Invalid file path");

   const size_t       LINES          { 1'000'000'000 };

   uint16_t           *stations      { nullptr };

   int32_t            *temperat      { nullptr },
                      *max           { nullptr },
                      *min           { nullptr }; 
   uint32_t           *count         { nullptr };

   unsigned long long *sum           { nullptr };

   int ifile                         { open(argv[1], O_RDONLY | O_LARGEFILE ) };
   if( ifile == -1){
        cerr << "Error: opening input file.\n";
        exit(EXIT_FAILURE);
   }

   posix_fadvise(ifile, 0, 0, POSIX_FADV_SEQUENTIAL);
   using Stat=struct stat;
   Stat           istat;
   fstat(ifile, &istat);
   ssize_t         isize  { istat.st_size };            

   unsigned char* idata  { static_cast<unsigned char*>(mmap(nullptr, isize, PROT_READ, MAP_PRIVATE |  MAP_POPULATE, ifile, 0)) };
   if( idata == MAP_FAILED){
        cerr << "Error: mmap : " << strerror(errno) << '\n';
        exit(EXIT_FAILURE);
   }

   if( cudaMallocManaged(&stations, LINES * sizeof(uint16_t)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (stations)\n";
        exit(EXIT_FAILURE);
   }

   if( cudaMallocManaged(&temperat, LINES * sizeof(int32_t)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (temperat)\n";
        exit(EXIT_FAILURE);
   }

   const size_t                   THREADS  { 32 };
   vector<vector<string>>         cities(THREADS);
   vector<unordered_set<string>>  singleCities(THREADS);
   vector<vector<int32_t>>        values(THREADS);

   
   vector<pair<size_t, size_t>>   iOffsets(THREADS);
   vector<pair<size_t, size_t>>   oOffsets(THREADS);
   const size_t                   START   { 0 },
                                  STOP    { 1 },
                                  SLICE   { isize / THREADS },
                                  DELTA   { 128 };

   cerr << "Start Slices calc.\n";
   for(size_t thr{ 0 }, prev{ 0 }, slice { SLICE }; thr < THREADS; thr++, slice+=SLICE){
        get<START>(iOffsets[thr])   = prev;
        size_t sl { slice };
        for( ; idata[sl] != '\n' && sl < isize ; sl++ ) {}
        get<STOP>(iOffsets[thr])    =  sl;
        prev                        =  sl + 1;
   }
   get<STOP>(iOffsets[THREADS - 1]) =  isize - 1;
   
   cerr << "End Slices calc.\n";
  
   const size_t                  BUFF_SIZE { 128 };
   auto worker { [&](size_t thrnum, size_t begin, size_t end) { 
                      string buff,
                             kkey;
                             
                      buff.reserve(BUFF_SIZE);
                      kkey.reserve(BUFF_SIZE);

                      cities[thrnum].reserve(SLICE + DELTA);
                      values[thrnum].reserve(SLICE + DELTA);

                      for(size_t idx { begin } ; idx < end ; idx++){
                            switch(idata[idx]){
                                case ';': 
                                       kkey = buff;
                                       buff.clear();
                                break;
                                case '\n': 
                                       buff.erase( buff.size() - 2, 1);
                                       cities[thrnum].push_back(kkey);
                                       singleCities[thrnum].insert(kkey);
                                       values[thrnum].push_back(stoi(buff));
                                       buff.clear();
                                break;
                                default: 
                                   buff.push_back(idata[idx]);
                            }
                      }
                 } 
   };

   vector<thread*>   workers(THREADS);
   for(size_t thr{ 0 }; thr < THREADS; thr++)
       workers[thr] = new thread(worker, thr, get<START>(iOffsets[thr]), get<STOP>(iOffsets[thr]));
   cerr << "Threads start.\n";

   for(size_t thr{ 0 }; thr < THREADS; thr++){
       workers[thr]->join();
       delete workers[thr];
   }
   cerr << "Threads end.\n";
 
   unordered_map<string, size_t> lookup;
   vector<string>                orderedLookup;
   unordered_set<string>         singleCitiesUnited;

   for(size_t idx{0}; idx < THREADS ; idx++){
      for(auto& elem: singleCities[idx])
         singleCitiesUnited.insert(elem);
   }

   cerr << "Start Output Offsets  calc.\n";
   for(size_t thr{ 0 }, prev{ 0 }; thr < THREADS; thr++){
        get<START>(oOffsets[thr])   =  prev;
        get<STOP>(oOffsets[thr])    =  prev + cities[thr].size() - 1;
        prev                        += cities[thr].size();
   }
   get<STOP>(oOffsets[THREADS -1])  =  LINES;
   cerr << "End Output Offsets  calc.\n";

   size_t     ord            { 0 };
   for(auto& city: singleCitiesUnited){
       if(! lookup.contains(city)){
            lookup[city] = ord;
            ord++;
            orderedLookup.push_back(city);
       }
   }

   auto c1 { high_resolution_clock::now() };
   auto loadVals { [&](size_t thrnum) { 
                             for(size_t idx { get<START>(oOffsets[thrnum]) }, didx { 0 } ; idx <= get<STOP>(oOffsets[thrnum]) ; idx++, didx++)
                                    *(temperat + idx ) = values[thrnum][didx];
                     }
   };
   cerr << "Loading sensors end.\n";

   auto loadCities { [&](size_t thrnum) { 
                             for(size_t idx { get<START>(oOffsets[thrnum]) }, didx { 0 } ; idx <= get<STOP>(oOffsets[thrnum]) ; idx++, didx++)
                                    *(stations + idx ) = lookup[cities[thrnum][didx]];
                        }
   };
   cerr << "Loading cities end.\n";

   for(size_t thr{ 0 }; thr < THREADS; thr++)
       workers[thr] = new thread(loadVals, thr );
   cerr << "Threads start.\n";

   for(size_t thr{ 0 }; thr < THREADS; thr++){
       workers[thr]->join();
       delete workers[thr];
   }

   for(size_t thr{ 0 }; thr < THREADS; thr++)
       workers[thr] = new thread(loadCities, thr );
   cerr << "Threads start.\n";

   for(size_t thr{ 0 }; thr < THREADS; thr++){
       workers[thr]->join();
       delete workers[thr];
   }
   cerr << "Threads end.\n";

   auto c2 { high_resolution_clock::now() };
   auto cms_int { duration_cast<milliseconds>(c2 - c1) };
   cerr << "\nCities loading  Execution Time: " << cms_int.count() << "ms\n\n";
        
   sort(orderedLookup.begin(), orderedLookup.end());

   const size_t OUTPUTSIZE { lookup.size() };
   if( cudaMallocManaged(&max, OUTPUTSIZE * sizeof(int32_t)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (max)\n";
        exit(EXIT_FAILURE);
   }

   if( cudaMallocManaged(&min, OUTPUTSIZE * sizeof(int32_t)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (min)\n";
        exit(EXIT_FAILURE);
   }

   if( cudaMallocManaged(&sum, OUTPUTSIZE * sizeof(unsigned long long)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (sum)\n";
        exit(EXIT_FAILURE);
   }

   if( cudaMallocManaged(&count, OUTPUTSIZE * sizeof(uint32_t)) != cudaSuccess){
        cerr << "Error: allocating unified memory  (count)\n";
        exit(EXIT_FAILURE);
   }

   const size_t BLOCKS { 256 },
                DIM    { (LINES + BLOCKS - 1) / BLOCKS };

   auto k1 { high_resolution_clock::now() };

   process<<<DIM, BLOCKS>>>(stations, temperat, LINES, max, min, sum, count);
   cudaDeviceSynchronize();

   auto k2 { high_resolution_clock::now() };
   auto kms_int { duration_cast<milliseconds>(k2 - k1) };
   cerr << "\nKernel Execution Time: " << kms_int.count() << "ms\n\n";

   string ofile{argv[1]};
   ofile.append(".out");
   ofstream output(ofile);
   for(auto& city : orderedLookup)
       output << city << ';' << min[lookup[city]] / 10.0 << ';' 
              << (long long)sum[lookup[city]] / count[lookup[city]] / 10.0 << ';' 
              << max[lookup[city]] / 10.0 << '\n';

   output.close();
   close(ifile);

   cudaFree(stations);
   cudaFree(temperat);
   cudaFree(max);
   cudaFree(min);
   cudaFree(sum);
   cudaFree(count);

   return EXIT_SUCCESS;
}
