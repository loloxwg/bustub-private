//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> guard(latch_);
  bool free_page = false;
  // check if there is a free page in the buffer pool manager instance
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() == 0) {
      free_page = true;
      break;
    }
  }
  // if there is no free page, return nullptr
  if (!free_page) {
    return nullptr;
  }
  // if there is a free page, allocate a new page id and return the page pointer
  *page_id = AllocatePage();  // get new page id from disk manager

  frame_id_t frame_id;
  // check if there is a free page in the free list
  if (!free_list_.empty()) {
    // if there is a free page in the free list, use it
    frame_id = free_list_.front();
    // remove the free page from the free list
    free_list_.pop_front();
  } else {
    // if there is no free page in the free list, use the page that is evictable
    replacer_->Evict(&frame_id);

    // if the page is dirty, write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    // reset the memory and metadata for the new page
    pages_[frame_id].ResetMemory();
    // reset the pin count for the new page
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  // insert the new page into the page table
  page_table_->Insert(*page_id, frame_id);
  // set the page id for the new page
  pages_[frame_id].page_id_ = *page_id;
  // set the pin count for the new page
  pages_[frame_id].pin_count_ = 1;

  // Pin the frame by calling replacer.SetEvictable(frame_id, false)
  // so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
  // Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // a. 使用自动获取和释放锁
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // check if the page is in the buffer pool manager instance
  if (page_table_->Find(page_id, frame_id)) {
    // if the page is in the buffer pool manager instance, return the page pointer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  bool free_page = false;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() == 0) {
      free_page = true;
      break;
    }
  }
  if (!free_page) {
    return nullptr;
  }

  // if the page is not in the buffer pool manager instance, check if there is a free page in the free list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // if there is no free page in the free list, use the page that is evictable
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    // if the page is dirty, write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    // reset the memory and metadata for the new page
    pages_[frame_id].ResetMemory();
    // reset the pin count for the new page
    page_table_->Remove(evicted_page_id);
  }
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush all the pages in the buffer pool to disk.
 */
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // if the page is pinned, return false
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // deleting the page from the page table
  page_table_->Remove(page_id);

  // stop tracking the frame in the replacer
  replacer_->Remove(frame_id);
  // add the frame back to the free list
  free_list_.push_back(frame_id);

  // reset the page's memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  // Finally, you should call DeallocatePage() to
  // imitate freeing the page on the disk.
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
