//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include <vector>
#include <queue>

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "util/stop_watch.h"

namespace leveldb {
namespace merger {
typedef std::priority_queue<
          IteratorWrapper*,
          std::vector<IteratorWrapper*>,
          MaxIteratorComparator> MaxIterHeap;

typedef std::priority_queue<
          IteratorWrapper*,
          std::vector<IteratorWrapper*>,
          MinIteratorComparator> MinIterHeap;

// Return's a new MaxHeap of IteratorWrapper's using the provided Comparator.
MaxIterHeap NewMaxIterHeap(const Comparator* comparator) {
  return MaxIterHeap(MaxIteratorComparator(comparator));
}

// Return's a new MinHeap of IteratorWrapper's using the provided Comparator.
MinIterHeap NewMinIterHeap(const Comparator* comparator) {
  return MinIterHeap(MinIteratorComparator(comparator));
}
}  // namespace merger

#if 0
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}
#endif

class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
                  
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        use_heap_(true),
        direction_(kForward),
        maxHeap_(merger::NewMaxIterHeap(comparator_)),
        minHeap_(merger::NewMinIterHeap(comparator_)) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (int i = 0; i < n; i++) {
      auto& child = children_[i];
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
  }
 
  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    ClearHeaps();
    for (int i = 0; i < n_; i++) {
      auto& child = children_[i];
      child.SeekToFirst();
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    ClearHeaps();
    for (int i = 0; i < n_; i++) {
      auto& child = children_[i];
      child.SeekToLast();
      if (child.Valid()) {
        maxHeap_.push(&child);
      }
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    // Invalidate the heap.
    use_heap_ = false;
    IteratorWrapper* first_child = nullptr;

    for (int i = 0; i < n_; i++) {
      auto& child = children_[i];
      {
        //(pmenon)PERF_TIMER_GUARD(seek_child_seek_time);
        child.Seek(target);
      }
      //(pmenon)PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        // This child has valid key
        if (!use_heap_) {
          if (first_child == nullptr) {
            // It's the first child has valid key. Only put it int
            // current_. Now the values in the heap should be invalid.
            first_child = &child;
          } else {
            // We have more than one children with valid keys. Initialize
            // the heap and put the first child into the heap.
            //(pmenon)PERF_TIMER_GUARD(seek_min_heap_time);
            ClearHeaps();
            minHeap_.push(first_child);
          }
        }
        if (use_heap_) {
          //(pmenon)PERF_TIMER_GUARD(seek_min_heap_time);
          minHeap_.push(&child);
        }
      }
    }
    if (use_heap_) {
      // If heap is valid, need to put the smallest key to curent_.
      //(pmenon)PERF_TIMER_GUARD(seek_min_heap_time);
      FindSmallest();
    } else {
      // The heap is not valid, then the current_ iterator is the first
      // one, or null if there is no first child.
      current_ = first_child;
    }
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      ClearHeaps();
      for (int i = 0; i < n_; i++) {
        auto& child = children_[i];
        if (&child != current_) {
          child.Seek(key());
          if (child.Valid() &&
              comparator_->Compare(key(), child.key()) == 0) {
            child.Next();
          }
          if (child.Valid()) {
            minHeap_.push(&child);
          }
        }
      }
      direction_ = kForward;
    }

    // as the current points to the current record. move the iterator forward.
    // and if it is valid add it to the heap.
    current_->Next();
    if (use_heap_) {
      if (current_->Valid()) {
        minHeap_.push(current_);
      }
      FindSmallest();
    } else if (!current_->Valid()) {
      current_ = nullptr;
    }
  }

  virtual void Prev() {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      ClearHeaps();
      for (int i = 0; i < n_; i++) {
        auto& child = children_[i];
        if (&child != current_) {
          child.Seek(key());
          if (child.Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child.Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child.SeekToLast();
          }
          if (child.Valid()) {
            maxHeap_.push(&child);
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    if (current_->Valid()) {
      maxHeap_.push(current_);
    }
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      auto& child = children_[i];
      status = child.status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();
  void ClearHeaps();

  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  // If the value is true, both of iterators in the heap and current_
  // contain valid rows. If it is false, only current_ can possibly contain
  // valid rows.
  // This flag is always true for reverse direction, as we always use heap for
  // the reverse iterating case.
  bool use_heap_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  merger::MaxIterHeap maxHeap_;
  merger::MinIterHeap minHeap_;
};

void MergingIterator::FindSmallest() {
  assert(use_heap_);
  if (minHeap_.empty()) {
    current_ = nullptr;
  } else {
    current_ = minHeap_.top();
    assert(current_->Valid());
    minHeap_.pop();
  }
}

void MergingIterator::FindLargest() {
  assert(use_heap_);
  if (maxHeap_.empty()) {
    current_ = nullptr;
  } else {
    current_ = maxHeap_.top();
    assert(current_->Valid());
    maxHeap_.pop();
  }
}

void MergingIterator::ClearHeaps() {
  use_heap_ = true;
  maxHeap_ = merger::NewMaxIterHeap(comparator_);
  minHeap_ = merger::NewMinIterHeap(comparator_);
}

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb 
