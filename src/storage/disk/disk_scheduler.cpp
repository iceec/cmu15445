//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
  //     "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

// 把这个任务 打包然后装进这个put进入队列里面就OK了
void DiskScheduler::Schedule(DiskRequest r) {
  // 先接收这个requeset 把他放进队列当中就OK了
  std::optional<DiskRequest> val = std::move(r);
  request_queue_.Put(std::move(val));
}

void DiskScheduler::handle_request(DiskRequest & request) 
{
  // 处理一个request
  // 处理写的话 那么就写就好了 要么转而调用读就好了
  if(request.is_write_)
  {
      disk_manager_->WritePage(request.page_id_,request.data_);
  }
  else
  {
    disk_manager_->ReadPage(request.page_id_,request.data_);
  }
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request = request_queue_.Get();
    // 没有值 表明调用了析构函数 那么返回
    if (!request.has_value()) break;

    handle_request(request.value());
    // 执行完成那么设置回到为true
    request.value().callback_.set_value(true);
  }
}

}  // namespace bustub
