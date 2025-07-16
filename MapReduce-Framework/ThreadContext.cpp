#include "ThreadContext.h"
#include <iostream>

ThreadContext::ThreadContext (int _id,
                              JobHandle _job_context,
                              pthread_t* _pthread_ptr) :
                              id (_id),
                              joined (false),
                              pthread_ptr (_pthread_ptr),
                              job_context (_job_context) { }


int ThreadContext::get_id () const
{
  return this->id;
}

ThreadContext::~ThreadContext()
{
  delete this->pthread_ptr;
}

pthread_t* ThreadContext::get_pthread_ptr ()
{
  return this->pthread_ptr;
}

JobHandle ThreadContext::get_job_context () const
{
  return this->job_context;
}

void ThreadContext::join_thread()
{
  if (!this->joined)
    {
      this->joined = true;
      if (pthread_join(*this->pthread_ptr, NULL) != 0)
        {
          std::cout << "system error: pthread_join problem" << std::endl;
          exit(1);
        }
    }
}
