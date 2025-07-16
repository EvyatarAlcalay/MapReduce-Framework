#include "JobContext.h"
#include <iostream>
#include <algorithm>

/***************************************/
/*       Functions Declarations        */
/***************************************/

float min(float a, float b);
bool comparator(IntermediatePair a, IntermediatePair b);
int count_different_keys(IntermediateVec* intermediate_vecs, int size);
bool not_in_vector(K2* val, const std::vector<K2*>& vector);
K2* find_largest_key(IntermediateVec* intermediate_vecs, int size);
void add_to_new_vec(K2* key, IntermediateVec* new_vec,
                     IntermediateVec* intermediate_vecs, int size);
bool is_same_key(K2* k_1, K2* k_2);
void lock_mutex(pthread_mutex_t* m);
void unlock_mutex(pthread_mutex_t* m);

/***************************************/
/*             Constructor             */
/***************************************/

JobContext::JobContext (int multiThreadLevel,
                        const MapReduceClient &_client,
                        const InputVec &_input_vec,
                        OutputVec &_output_vec):
    state ({UNDEFINED_STAGE, 0}),
    size_of_intermediate_vecs (multiThreadLevel),
    atomic_counter (0),
    atomic_counter_percentage (0),
    mutex (),
    client (_client),
    input_vec (_input_vec),
    intermediate_vecs (new IntermediateVec[multiThreadLevel]),
    output_vec (_output_vec),
    barrier (multiThreadLevel) {}


JobContext::~JobContext ()
{
  delete[] this->intermediate_vecs;
  for (auto thread : this->thread_contexts)
  {
    delete (ThreadContext*) thread;
  }
}


/***************************************/
/*               Getters               */
/***************************************/

Barrier& JobContext::get_barrier ()
{
  return this->barrier;
}

JobState JobContext::get_job_state () const
{
  return this->state;
}

const MapReduceClient& JobContext::get_client()
{
  return this->client;
}

const InputVec& JobContext::get_input_vec()
{
  return this->input_vec;
}

IntermediateVec* JobContext::get_intermediate_vecs ()
{
  return this->intermediate_vecs;
}

int JobContext::get_size_of_intermediate_vecs() const
{
  return this->size_of_intermediate_vecs;
}

int JobContext::get_and_increase_atomic_counter ()
{
  int old_value = (this->atomic_counter)++;
  return old_value;
}

thread_context_vec& JobContext::get_thread_contexts()
{
  return this->thread_contexts;
}

/***************************************/
/*               Setters               */
/***************************************/

void JobContext::push_new_thread (void *new_thread_context)
{
  this->thread_contexts.push_back (new_thread_context);
}

void JobContext::set_state (JobState new_state)
{
  this->state = new_state;
}

void JobContext::reset_atomic_counter ()
{
  this->atomic_counter = 0;
  this->atomic_counter_percentage = 0;
}

void JobContext::set_percentage ()
{
  lock_mutex (&mutex);
  auto val = (float) ++(this->atomic_counter_percentage);
  switch (this->state.stage)
    {
      case MAP_STAGE:
        this->state.percentage =
            min(val / (float)this->input_vec.size(),1.0) * 100;
        break;
      case SHUFFLE_STAGE:
        this->state.percentage =
        min(val / (float)size_of_intermediate_vecs,1.0) * 100;
        break;
      case REDUCE_STAGE:
        this->state.percentage =
            min(val / (float)size_of_intermediate_vecs,1.0) * 100;
        break;
      default:
        this->state.percentage = 0;

    }
  unlock_mutex (&mutex);
}

void JobContext::add_intermediate_pair(K2* key, V2* value, int id)
{
  this->intermediate_vecs[id].push_back (std::make_pair (key, value));
}

/***************************************/
/*                Other                */
/***************************************/

void JobContext::sort_intermediate_vec (int id)
{
  auto& v = this->intermediate_vecs[id];
  std::sort(v.begin(), v.end(), &comparator);
}

void JobContext::shuffle ()
{
  int count = count_different_keys(this->intermediate_vecs,
                                   this->size_of_intermediate_vecs);
  auto new_vecs = new IntermediateVec[count];
  int old_size = this->size_of_intermediate_vecs;
  this->size_of_intermediate_vecs = count;
  for (int i = 0; i < count; i++)
    {
      auto key = find_largest_key(this->intermediate_vecs, old_size);
      add_to_new_vec (key, new_vecs + i, this->intermediate_vecs, old_size);
      this->set_percentage();
    }
  delete[] this->intermediate_vecs;
  this->intermediate_vecs = new_vecs;
}

void JobContext::add_output_pair(K3* key, V3* value)
{
  lock_mutex (&mutex);
  this->output_vec.push_back (std::make_pair (key, value));
  unlock_mutex (&mutex);
}

/***************************************/
/*               Helpers               */
/***************************************/


float min(float a, float b)
{
  if (a < b)
    {
      return a;
    }
  return b;
}

bool comparator(IntermediatePair a, IntermediatePair b)
{
  return (*a.first) < (*b.first);
}

int count_different_keys(IntermediateVec* intermediate_vec, int size)
{
  std::vector<K2*> all_keys_vec;
  for (int i = 0; i < size; i++)
    {
      auto& v = intermediate_vec[i];
      for (const auto & elem : v)
        {
            if (not_in_vector (elem.first, all_keys_vec))
              {
                all_keys_vec.push_back (elem.first);
              }
        }
    }
  return (int) all_keys_vec.size();
}

bool not_in_vector(K2* val, const std::vector<K2*>& vector)
{
  for (const auto& v : vector)
    {
      if (is_same_key (v, val))
        {
          return false;
        }
    }
  return true;
}

bool is_same_key(K2* k_1, K2* k_2)
{
  return !(*k_1 < *k_2) && !(*k_2 < *k_1);
}

K2* find_largest_key(IntermediateVec* intermediate_vecs, int size)
{
  if (intermediate_vecs == nullptr || size == 0)
  {
    return nullptr;
  }
  K2* largest_key = nullptr;
  for (int i = 0; i < size; i++)
  {
    auto& v = intermediate_vecs[i];
    if (v.empty())
    {
      continue;
    }
    auto& curr_key = v.at(v.size() - 1).first;
    if ((largest_key == nullptr) || (*largest_key < *(curr_key)))
    {
      largest_key = curr_key;
    }
  }
  return largest_key;
}

void add_to_new_vec(K2* key, IntermediateVec* new_vec,
                     IntermediateVec* intermediate_vecs, int size)
{
  if (intermediate_vecs == nullptr || size == 0 || new_vec == nullptr)
  {
    return;
  }

  for (int i = 0; i < size; i++)
  {
    auto& v = intermediate_vecs[i];
    if (v.empty())
    {
      continue;
    }
    auto& curr_elem = v.at(v.size() - 1);
    while (is_same_key (key, curr_elem.first))
    {
      new_vec->push_back (curr_elem);
      v.pop_back();
      if (v.empty())
        {
          break;
        }
      curr_elem = v.at(v.size() - 1);
    }
  }
}

void lock_mutex(pthread_mutex_t* m)
{
  if(pthread_mutex_lock (m) != 0)
    {
      std::cout << "system error: mutex lock problem" << std::endl;
      exit(1);
    }
}

void unlock_mutex(pthread_mutex_t* m)
{
  if(pthread_mutex_unlock (m) != 0)
    {
      std::cout << "system error: mutex unlock problem" << std::endl;
      exit(1);
    }
}