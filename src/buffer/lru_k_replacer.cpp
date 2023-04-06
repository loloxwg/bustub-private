//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict the frame with the earliest
 * timestamp overall.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @param[out] frame_id id of frame that is evicted.
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  // if outer list is not empty, then evict from outer list first
  //  reverse iterator is used to iterate from the end of the list
  //  if frame is evictable, then evict it
  for (auto it = outer_list_.rbegin(); it != outer_list_.rend(); it++) {
    auto frame = *it;
    // if frame is evictable, then evict it
    if (evictable_map_[frame]) {
      // remove frame from outer list
      access_record_map_.erase(frame);  // reset access record history to 0
      outer_list_.erase(outer_index_[frame]);
      outer_index_.erase(frame);
      *frame_id = frame;  // return frame id
      curr_size_--;
      evictable_map_.erase(frame);  // set evictable to false
      return true;
    }
  }

  // if outer list is empty, then evict from pool cache list
  for (auto it = pool_cache_list_.rbegin(); it != pool_cache_list_.rend(); it++) {
    auto frame = *it;
    if (evictable_map_[frame]) {
      access_record_map_.erase(frame);
      pool_cache_list_.erase(pool_cache_index_[frame]);
      pool_cache_index_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      evictable_map_.erase(frame);
      return true;
    }
  }

  return false;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    LOG_ERROR("frame_id is invalid");
    throw std::exception();
  }
  access_record_map_[frame_id]++;  // increase access record by 1

  // if access record is less than k, then it is in outer list
  if (access_record_map_[frame_id] < k_) {
    // if it is not in outer list, then insert it into outer list
    if (outer_index_.count(frame_id) == 0U) {
      // push front to outer list
      outer_list_.push_front(frame_id);
      outer_index_[frame_id] = outer_list_.begin();
    }
  }
  // if access record is equal to k,
  // then it is erased from outer list
  // and insert into pool cache list
  else if (access_record_map_[frame_id] == k_) {
    auto it = outer_index_[frame_id];
    outer_list_.erase(it);
    outer_index_.erase(frame_id);
    pool_cache_list_.push_front(frame_id);
    pool_cache_index_[frame_id] = pool_cache_list_.begin();
  }
  // if access record is greater than k
  else {
    // if it has already in pool cache list,
    // first erase it from pool cache list
    // then insert it into pool cache list
    if (pool_cache_index_.count(frame_id) != 0U) {
      auto iter = pool_cache_index_[frame_id];
      pool_cache_list_.erase(iter);
    }
    // insert it into pool cache list
    pool_cache_list_.push_front(frame_id);
    pool_cache_index_[frame_id] = pool_cache_list_.begin();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // if frame id is invalid, throw an exception
  if (frame_id >= static_cast<int>(replacer_size_)) {
    LOG_ERROR("frame_id is invalid");
    throw std::exception();
  }
  // if frame id is not in access record map, then return directly
  if (access_record_map_[frame_id] == 0) {
    return;
  }
  // if frame id is in access record map, then set evictable or not
  if (evictable_map_[frame_id] == set_evictable) {
    return;
  }
  if (set_evictable) {
    evictable_map_[frame_id] = true;
    curr_size_++;
  } else {
    evictable_map_[frame_id] = false;
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    LOG_ERROR("frame_id is invalid");
    throw std::exception();
  }
  if (access_record_map_[frame_id] == 0) {
    return;
  }

  if (!evictable_map_[frame_id]) {
    LOG_ERROR("frame_id is not evictable");
    throw std::exception();
  }
  if (access_record_map_[frame_id] < k_) {
    auto it = outer_index_[frame_id];
    outer_list_.erase(it);
    outer_index_.erase(frame_id);
  } else {
    auto it = pool_cache_index_[frame_id];
    pool_cache_list_.erase(it);
    pool_cache_index_.erase(frame_id);
  }
  access_record_map_.erase(frame_id);
  evictable_map_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_DEBUG("curr_size_ = %ld", curr_size_);
  return curr_size_;
}

}  // namespace bustub
