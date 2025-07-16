#include <iostream>
#include <unistd.h>
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "ThreadContext.h"

typedef void* JobHandle;

/***************************************/
/*       Functions Declarations        */
/***************************************/

void* routine (void* arg);
void init_map (ThreadContext* thread_context);
void init_reduce (ThreadContext* thread_context);
void do_map(ThreadContext* thread_context);
void do_shuffle(ThreadContext* thread_context);
void do_reduce(ThreadContext* thread_context);

/***************************************/
/*          H IMPLEMENTATIONS          */
/***************************************/

void emit2 (K2* key, V2* value, void* context)
{
  auto thread = (ThreadContext*) context;
  auto job = (JobContext*) thread->get_job_context();

  job->add_intermediate_pair (key, value, thread->get_id());
}

void emit3 (K3* key, V3* value, void* context)
{
  auto thread = (ThreadContext*) context;
  auto job = (JobContext*) thread->get_job_context();

  job->add_output_pair(key, value);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
  auto job_context = new JobContext(multiThreadLevel,
                                client,
                                inputVec,
                                outputVec);
  for (int i = 0; i < multiThreadLevel; i++)
    {
      auto thread_context = new ThreadContext(i, job_context, new pthread_t);
      if (pthread_create (thread_context->get_pthread_ptr (),
                      NULL,
                      routine,
                      thread_context) != 0)
        {
          std::cout << "system error: creation of thread failed, to many threads" << std::endl;;
          exit(1);
        }
      job_context->push_new_thread (thread_context);
    }
  return job_context;
}

void waitForJob(JobHandle job)
{
  {
    thread_context_vec& all_threads = ((JobContext*) job)->get_thread_contexts();
    for (auto thread : all_threads)
      {
        ((ThreadContext*) thread)->join_thread();
      }
    }
}

void getJobState(JobHandle job, JobState* state)
{
  *state = ((JobContext*) job)->get_job_state();
}

void closeJobHandle(JobHandle job)
{
  waitForJob (job);
  delete (JobContext*) job;
}

/***************************************/
/*               Helpers               */
/***************************************/

void* routine (void* arg)
{
  auto thread = (ThreadContext*) arg;
  auto job = (JobContext*) thread->get_job_context();

  //map and sort
  job->get_barrier().barrier();
  init_map (thread);
  job->get_barrier().barrier();
  do_map (thread);

  //shuffle
  job->get_barrier().barrier();
  do_shuffle (thread);

  //reduce
  job->get_barrier().barrier();
  init_reduce (thread);
  job->get_barrier().barrier();
  do_reduce (thread);
  return nullptr;
}

/***************************************/
/*           Routine Helpers           */
/***************************************/

void init_map (ThreadContext* thread_context)
{
  if (thread_context->get_id() == 0)
    {
      auto job = (JobContext*) thread_context->get_job_context();
      job->set_state ({MAP_STAGE, 0});
      job->reset_atomic_counter();
    }
}

void do_map(ThreadContext* thread_context)
{
  auto job = (JobContext*) thread_context->get_job_context();
  const InputVec& input_vec = job->get_input_vec();
  int val = job->get_and_increase_atomic_counter();
  while (val < (int) input_vec.size())
    {
      auto& input_pair = input_vec.at (val);
      job->get_client().map (input_pair.first, input_pair.second, thread_context);
      job->set_percentage();
      val = job->get_and_increase_atomic_counter();
    }
  job->sort_intermediate_vec (thread_context->get_id());
}

void do_shuffle (ThreadContext* thread_context)
{
  if (thread_context->get_id() == 0)
    {
      auto job = (JobContext*) thread_context->get_job_context();
      job->set_state ({SHUFFLE_STAGE, 0});
      job->reset_atomic_counter();
      job->shuffle();
    }
}

void init_reduce (ThreadContext* thread_context)
{
  if (thread_context->get_id() == 0)
    {
      auto job = (JobContext*) thread_context->get_job_context();
      job->set_state ({REDUCE_STAGE, 0});
      job->reset_atomic_counter();
    }
}

void do_reduce(ThreadContext* thread_context)
{
  auto job = (JobContext*) thread_context->get_job_context();
  auto intermediate_vecs = job->get_intermediate_vecs();
  int val = job->get_and_increase_atomic_counter();
  int size = job->get_size_of_intermediate_vecs();
  while (val < size)
    {
      job->get_client().reduce(intermediate_vecs + val, thread_context);
      job->set_percentage();
      val = job->get_and_increase_atomic_counter();
    }
}
