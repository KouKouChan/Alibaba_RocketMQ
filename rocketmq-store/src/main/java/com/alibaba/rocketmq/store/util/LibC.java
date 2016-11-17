/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.store.util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * JNA interface to access native system calls.
 */
public interface LibC extends Library {

    LibC INSTANCE = (LibC) Native.loadLibrary(Platform.isWindows() ? "msvcrt" : "c", LibC.class);

    /**
     * No special treatment.  This is the default.
     */
    int MADV_NORMAL = 0;

    /**
     * Expect page references in random order.  (Hence, read ahead may be less useful than normally.)
     */
    int MADV_RANDOM = 1;

    /**
     * Expect page references in sequential order.  (Hence, pages in
     * the given range can be aggressively read ahead, and may be
     * freed soon after they are accessed.)
     */
    int MADV_SEQUENTIAL = 2;

    /**
     * Expect access in the near future. (Hence, it might be a good idea to read some pages ahead.)
     */
    int MADV_WILLNEED = 3;

    int MADV_DONTNEED = 4;

    /**
     * locks pages in the address range starting at addr and continuing for len bytes. All pages that contain a part of
     * the specified address range are guaranteed to be resident in RAM when the call returns successfully; the pages
     * are guaranteed to stay in RAM until later unlocked.
     *
     * @param addr
     * @param len
     * @return On success this system call returns 0. On error, -1 is returned, errno is set appropriately, and no
     * changes are made to any locks in the address space of the process.
     */
    int mlock(Pointer addr, NativeLong len);

    /**
     * unlocks pages in the address range starting at addr and continuing for len bytes. After this call, all pages
     * that contain a part of the specified memory range can be moved to external swap space again by the kernel.
     * @param addr
     * @param len
     * @return On success this system call returns 0. On error, -1 is returned, errno is set appropriately, and no
     * changes are made to any locks in the address space of the process.
     */
    int munlock(Pointer addr, NativeLong len);

    /**
     * The madvise() system call is used to give advice or directions to the
     * kernel about the address range beginning at address addr and with
     * size length bytes.  Initially, the system call supported a set of
     * "conventional" advice values, which are also available on several
     * other implementations.  (Note, though, that madvise() is not
     * specified in POSIX.)  Subsequently, a number of Linux-specific advice
     * values have been added.
     * @param var1 Pointer to memory.
     * @param var2 Length of the memory to advise
     * @param var3 Flag, see http://man7.org/linux/man-pages/man2/madvise.2.html
     * @return On success, madvise() returns zero.  On error, it returns -1 and errno is set appropriately.
     */
    int madvise(Pointer var1, NativeLong var2, int var3);
}
