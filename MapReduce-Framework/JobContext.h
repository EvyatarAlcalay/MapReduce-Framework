#ifndef JOB_CONTEXT_H
#define JOB_CONTEXT_H

#include <memory>
#include <pthread.h>
#include <vector>
#include <atomic>
#include "MapReduceFramework.h"
#include "ThreadContext.h"

typedef std::vector<void* > thread_context_vec;

class JobContext{
 private:
  JobState state;
  int size_of_intermediate_vecs;
  std::atomic<int> atomic_counter;
  std::atomic<int> atomic_counter_percentage;
  pthread_mutex_t mutex;
  const MapReduceClient& client;
  const InputVec &input_vec;
  IntermediateVec* intermediate_vecs;
  OutputVec &output_vec;
  Barrier barrier;
  thread_context_vec thread_contexts;


 public:
  //constructor
  JobContext(int multiThreadLevel,
             const MapReduceClient& _client,
             const InputVec& _input_vec,
             OutputVec& _output_vec);

  //destructors
  ~JobContext();

  //getters
  Barrier& get_barrier();
  JobState get_job_state() const;
  const MapReduceClient& get_client();
  const InputVec& get_input_vec();
  IntermediateVec* get_intermediate_vecs();
  int get_and_increase_atomic_counter();
  int get_size_of_intermediate_vecs() const;
  thread_context_vec& get_thread_contexts();
  void push_new_thread(void* new_thread_context);

  //setters
  void set_state(JobState new_state);
  void reset_atomic_counter();
  void set_percentage ();

  //others
  void add_intermediate_pair(K2* key, V2* value, int id);
  void sort_intermediate_vec(int id);
  void shuffle();
  void add_output_pair(K3* key, V3* value);
};

#endif //JOB_CONTEXT_H