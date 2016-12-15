
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

class filewriter {
public:
  const uint32_t MSG_LOCALLY_STABLE = 0x1;
  const uint32_t MSG_GLOBAL_ORDERED = 0x2;
  const uint32_t MSG_GLOBAL_STABLE = 0x4;
  const uint32_t MSG_LOCALLY_PERSISTENT = 0x8;

  struct message {
    char *data;
    uint64_t length;

    uint32_t sender;
    uint64_t message_number;
  };

private:
  struct __attribute__((__packed__)) header {
    uint8_t magic[8];
    uint32_t version;
  };
  struct __attribute__((__packed__)) message_metadata {
    uint32_t sender;
    uint32_t padding;

    uint64_t message_number;

    uint64_t offset;
    uint64_t length;
  };

  const std::function<void(message)> message_written_upcall;

  std::thread writer_thread;
  std::thread callback_thread;

  std::queue<message> pending_writes;
  std::mutex pending_writes_mutex;
  std::condition_variable pending_writes_cv;

  std::queue<std::function<void()>> pending_callbacks;
  std::mutex pending_callbacks_mutex;
  std::condition_variable pending_callbacks_cv;

  std::atomic<bool> exit;

  void perform_writes(std::string filename);
  void issue_callbacks();

public:
  filewriter(std::function<void(message)> _message_written_upcall,
             std::string filename);
  ~filewriter();

  filewriter(filewriter &) = delete;
  filewriter(filewriter &&) = default;

  filewriter &operator=(filewriter &) = delete;
  filewriter &operator=(filewriter &&) = default;

  void write_message(message m);
};
