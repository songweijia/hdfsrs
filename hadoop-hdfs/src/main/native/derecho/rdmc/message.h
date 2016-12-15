
#ifndef MESSAGE_H
#define MESSAGE_H

#include <cstdint>
#include <utility>

enum class MessageType : uint8_t {
    DATA_BLOCK = 0x01,
    SMALL_MESSAGE = 0x02,
    READY_FOR_BLOCK = 0x03,
    BARRIER = 0x04,
};

struct ParsedTag {
    MessageType message_type;
    uint16_t group_number;
    uint32_t target;
};

inline ParsedTag parse_tag(uint64_t t) {
    return ParsedTag{(MessageType)((t & 0x00ff000000000000ull) >> 48),
                     (uint16_t)((t & 0x0000ffff00000000ull) >> 32),
                     (uint32_t)(t & 0x00000000ffffffffull)};
}
inline uint64_t form_tag(uint16_t group_number, uint32_t target,
                         MessageType message_type) {
    return (((uint64_t)message_type) << 48) | (((uint64_t)group_number) << 32) |
           (uint64_t)target;
}

struct ParsedImmediate {
    uint16_t total_blocks;
    uint16_t block_number;
};

inline ParsedImmediate parse_immediate(uint32_t imm) {
    return ParsedImmediate{(uint16_t)((imm & 0xffff0000) >> 16),
                           (uint16_t)(imm & 0x0000ffff)};
}
inline uint32_t form_immediate(uint16_t total_blocks, uint16_t block_number) {
    return ((uint32_t)total_blocks) << 16 | ((uint32_t)block_number);
}

#endif
