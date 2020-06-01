#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include <math.h>
#include <algorithm>



void clearTable(uint64_t frameIndex) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize() {
    clearTable(0);
}

/**
 * assign to result the number of LSB (bitsNum) of the given address (such as offset)
 * @param address - the address to parse
 * @param bitsNum - number of bits to parse from address
 * @param result - a variable to assign the result to
 */
void getLSB(const uint64_t &address, const int &bitsNum, int &result) {
    result = address & (word_t)(pow(2, bitsNum) - 1);
}

/**
 * assign to result the number of MSB (bitsNum) of the given address
 * @param address - the address to parse
 * @param bitsNum - number of bits to parse from address
 * @param result - a variable to assign the result to
 */
void getMSB(const uint64_t &address, const int &bitsNum, int &result) {
    result = address >> bitsNum;       // equals to address/(pow(2, bitsNum))
}

/**
 * the function gets an array and fill it with parsed addresses.
 * @param virtualAddress
 * @param addresses
 * @param offset
 */
void parseAddresses(uint64_t virtualAddress, int addresses[TABLES_DEPTH], int &offset) {
    getLSB(virtualAddress, OFFSET_WIDTH, offset);                           // insert offset address to offset var
    virtualAddress = virtualAddress >> OFFSET_WIDTH;                        // cut offset off of virtualAddress

    for (int i = TABLES_DEPTH - 1; i >= 0; i--) {                           // insert the p_i to the i'th cell
        getLSB(virtualAddress, OFFSET_WIDTH, addresses[i]);                 // assuming offset_width = p_i width
        virtualAddress = virtualAddress >> OFFSET_WIDTH;                    // cut p_i off of virtualAddress
    }
}

/**
 * the function find (or creates/evicts) a frame to use for reading/writing.
 * @param givenPageVAddress - a virtual address of the page we want to read from / write to eventually.
 * @param currentVAddress - the virtual address of the current vertex we're in (only relevant for leafs).
 * @param emptyFrameNum - our first priority is to find an empty frame, so if we found such frame its
 *                      number will store in this var, else it would be 0.
 * @param curDepth - current depth in the table tree.
 * @param maxDistance - the maximal distance measured so far of a leaf from the leaf we want to get to.
 * @param parentFrameNumber - the frame number of the current vertex's parent.
 * @param frameNumberToEvict - the optimal page number to evict from PM.
 * @param pageToEvictVAddress - the virtual address of the optimal page to evict.
 * @param frameToEvictReference - the current frame's parent's physical address
 * @param maxFilledFrameNum - the maximal number of not-empty frame we visited by now.
 * @param lastRestoredFrame - the number of the last frame (table) we filled in our journey.
 */
void findEmptyFrame(const uint64_t givenPageVAddress,
                    const uint64_t currentVAddress,
                    const int curFrameNum,
                    int &emptyFrameNum,
                    const int curDepth,
                    int &maxDistance,
                    const int parentFrameNumber,
                    int &frameNumberToEvict,
                    uint64_t &pageToEvictVAddress,
                    uint64_t &frameToEvictReference,
                    int &maxFilledFrameNum,
                    const int lastRestoredFrame){

    if(emptyFrameNum != 0) {                                                // found an empty frame (without children in physical memory)
        return;
    }
    else if (curDepth == TABLES_DEPTH){                                     // if it's a leaf
        long long int distance = std::min(NUM_PAGES - abs((long long int)(givenPageVAddress - currentVAddress)),
                abs((long long int) (givenPageVAddress - currentVAddress)));
        if (distance > maxDistance){                                        // if we found a leaf with bigger distance from pageToEvict
            maxDistance = distance;
            pageToEvictVAddress = currentVAddress;
            int offset = 0;
            getLSB(currentVAddress, OFFSET_WIDTH, offset);                  // get the offset of current vertex in its parent's frame
            PMread((uint64_t) (parentFrameNumber * PAGE_SIZE + offset), &frameNumberToEvict);
            frameToEvictReference = PAGE_SIZE * parentFrameNumber + offset; //calculate the physical address of the parent which points to FrameToEvict
        }
    }
    else {      // not a leaf (just a table in the tree)
        bool isFrameEmpty = true;                                           // true if the table in the current frame is empty (all 0)
        word_t childFrameNum = 0;
        for (int i = 0; i < PAGE_SIZE; ++i) {                               // for each child of current vertex
            PMread(PAGE_SIZE * curFrameNum + i, &childFrameNum);            // read the content of the child frame from his parent's frame (in physical memory)
            if (childFrameNum != 0){                                        // if the child has a frame in physical memory
                isFrameEmpty = false;                                       // frame isn't empty because it has a child in physical memory
                if ((int)childFrameNum > maxFilledFrameNum){                // update max filled frame number if needed
                    maxFilledFrameNum = (int) childFrameNum;
                }
                uint64_t childVAddress = (uint64_t) (currentVAddress * pow(2,OFFSET_WIDTH) + i);
                findEmptyFrame(givenPageVAddress,
                               childVAddress, //currentVAddress
                               childFrameNum, //curFrameNum,
                               emptyFrameNum,
                               curDepth + 1,
                               maxDistance,
                               curFrameNum, //parentFrameNumber
                               frameNumberToEvict,
                               pageToEvictVAddress,
                               frameToEvictReference,
                               maxFilledFrameNum,
                               lastRestoredFrame);
            }
        }

        if (isFrameEmpty && lastRestoredFrame != curFrameNum) {     // if the empty frame we found isn't the recently created one
            int offset = 0;
            getLSB(currentVAddress, OFFSET_WIDTH, offset);          // get the offset of current vertex in its parent's frame
            PMwrite(parentFrameNumber * PAGE_SIZE + offset, 0);     // write 0 in parent at the child's index (=offset)
            emptyFrameNum = curFrameNum;                            // assigning the empty frame we found to frameNum
            return;
        }
    }
}


/**
 * we'll find an empty page (or evict one if necessary and if != parent's frame)
 * @param givenVAddress - the given virtual address to calculate distance of leafs from it (cyclic distance).
 * @param lastRestoredFrame - the address of the parent which we don't want to evict.
 * @param offsetInParentFrame - the offset of the page in parent frame.
 * @param emptyFrameNum - the address in some frame to put the empty page in.
 */
void findFrame(const uint64_t givenVAddress,
               const int lastRestoredFrame,
               const uint64_t &offsetInParentFrame,
               int &emptyFrameNum) {

    const uint64_t givenPageVAddress = givenVAddress >> OFFSET_WIDTH;
    const uint64_t currentVAddress = 0;
    int curFrameNum = 0;
    int curDepth = 0;
    int maxDistance = 0;
    int parentFrameNumber = 0;
    int frameNumberToEvict = 0;
    uint64_t pageToEvictVAddress = 0;
    uint64_t frameToEvictReference = 0;
    int maxFilledFrameNum = 0;

    findEmptyFrame(givenPageVAddress,
                   currentVAddress,
                   curFrameNum,
                   emptyFrameNum,
                   curDepth,
                   maxDistance,
                   parentFrameNumber,
                   frameNumberToEvict,
                   pageToEvictVAddress,
                   frameToEvictReference,
                   maxFilledFrameNum,
                   lastRestoredFrame);              // try to find an empty frame

    if (emptyFrameNum == 0) {                       // didn't find frame without children in physical memory
        if (maxFilledFrameNum < NUM_FRAMES - 1){    // there is an empty frame
            emptyFrameNum = maxFilledFrameNum + 1;
        }
        else {                                      // evict a frame and write 0 in parent at the child's index
            PMevict(frameNumberToEvict, pageToEvictVAddress);
            emptyFrameNum = frameNumberToEvict;
            PMwrite(frameToEvictReference, 0);      // write 0 in parent's reference to the frame to evict
        }
    }
    // update the address we found in the right place at parent's frame
    PMwrite(PAGE_SIZE * lastRestoredFrame + offsetInParentFrame, emptyFrameNum);
}

/**
 * restores the page we want to read/write to from the physical memory
 * @param virtualAddress - the virtual address of the page we want to read or write to
 * @param offset - the offset of the page we want to read or write to at its parent
 * @param emptyFrameNum - the frame we eventually put our desired frame into
 */
void VMrestore(uint64_t virtualAddress, int &offset, word_t &emptyFrameNum){
    int addresses[TABLES_DEPTH] = {0};                                                  // create an array of addresses p_1.....p_n
    parseAddresses(virtualAddress, addresses, offset);                                  // fill the array

    word_t parentFrameNum = 0;

    for (int i = 0; i < TABLES_DEPTH; ++i) {
        PMread((uint64_t)(parentFrameNum * PAGE_SIZE + addresses[i]), &emptyFrameNum);  // digging in the tree (getting the next address)
        if (emptyFrameNum == 0) {                                                       // the next table isn't in the physical memory
            findFrame(virtualAddress, parentFrameNum, addresses[i], emptyFrameNum);
            if (i != TABLES_DEPTH - 1) {                                                // if we want to store a table in an empty frame in PM
                clearTable(emptyFrameNum);                                              // we need to zero its content
            } else {
                // when we're at a leaf which we want to read from - pulling the its page from the hard-drive into the PM
                PMrestore(emptyFrameNum, virtualAddress >> OFFSET_WIDTH);
            }
        }
        parentFrameNum = emptyFrameNum;
    }
}


int VMread(uint64_t virtualAddress, word_t* value) {
    int offset = 0;
    word_t emptyFrameNum = 0;

    VMrestore(virtualAddress, offset, emptyFrameNum);

    PMread(emptyFrameNum * PAGE_SIZE + offset, value);              // read value from physical memory
    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value) {
    int offset = 0;
    word_t emptyFrameNum = 0;

    VMrestore(virtualAddress, offset, emptyFrameNum);

    PMwrite(emptyFrameNum * PAGE_SIZE + offset, value);             // write value to physical memory
    return 1;
}







