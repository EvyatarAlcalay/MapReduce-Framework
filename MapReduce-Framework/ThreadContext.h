#ifndef TREAD_CONTEXT_H
#define TREAD_CONTEXT_H

#include <memory>
#include <pthread.h>
#include "Barrier.h"
#include "MapReduceClient.h"
#include "JobContext.h"

class ThreadContext
{
 private:
  int id;
  bool joined;
  pthread_t* pthread_ptr;
  JobHandle job_context;

 public:
  //constructors
  ThreadContext (int _id, JobHandle _job_context, pthread_t* _pthread_ptr);

  //destructors
  ~ThreadContext();

  //getters
  int get_id() const;
  pthread_t* get_pthread_ptr();
  JobHandle get_job_context() const;

  //others
  void join_thread();


};

#endif //TREAD_CONTEXT_H