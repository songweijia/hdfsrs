
#include "filewriter.h"

#include <cstring>
#include <thread>

using namespace std;

const uint8_t MAGIC_NUMBER[8] = {'D', 'E', 'R', 'E', 'C', 'H', 'O', 29};

filewriter::filewriter(std::function<void(message)> _message_written_upcall,
                       string filename)
    : message_written_upcall(_message_written_upcall),
      writer_thread(&filewriter::perform_writes, this, filename),
      callback_thread(&filewriter::issue_callbacks, this) {}

filewriter::~filewriter() {
  exit = true;
  pending_callbacks_cv.notify_all();
  pending_writes_cv.notify_all();
  if(writer_thread.joinable())
	  writer_thread.join();
  if(callback_thread.joinable())
	  callback_thread.join();
}

void filewriter::perform_writes(string filename) {
  ofstream data_file(filename);
  ofstream metadata_file(filename + ".metadata");

  unique_lock<mutex> lock(pending_writes_mutex);

  uint64_t current_offset = 0;

  header h;
  memcpy(h.magic, MAGIC_NUMBER, sizeof(MAGIC_NUMBER));
  h.version = 0;
  metadata_file.write((char *)&h, sizeof(h));

  while (!exit) {
    pending_writes_cv.wait(lock);

    if (!pending_writes.empty()) {
      message m = pending_writes.front();
      pending_writes.pop();

      message_metadata metadata;
      metadata.sender = m.sender;
      metadata.message_number = m.message_number;
      metadata.offset = current_offset;
      metadata.length = m.length;

      data_file.write(m.data, m.length);
      metadata_file.write((char *)&metadata, sizeof(metadata));

      data_file.flush();
      metadata_file.flush();

      current_offset += m.length;

      {
        unique_lock<mutex> lock(pending_callbacks_mutex);
        pending_callbacks.push(std::bind(message_written_upcall, m));
      }
      pending_callbacks_cv.notify_all();
    }
  }
}

void filewriter::issue_callbacks() {
  unique_lock<mutex> lock(pending_callbacks_mutex);

  while (!exit) {
    pending_callbacks_cv.wait(lock);

    if (!pending_callbacks.empty()) {
      auto callback = pending_callbacks.front();
      pending_callbacks.pop();
      callback();
    }
  }
}

void filewriter::write_message(message m) {
  {
    unique_lock<mutex> lock(pending_writes_mutex);
    pending_writes.push(m);
  }
  pending_writes_cv.notify_all();
}
