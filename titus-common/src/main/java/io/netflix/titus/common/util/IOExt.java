/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.common.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * IO utils.
 */
public final class IOExt {

    private IOExt() {
    }

    /**
     * Close the given resource, swallowing any emitted exceptions.
     */
    public static void closeSilently(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {
            }
        }
    }

    /**
     * Read all lines from the given file, using default system character encoding.
     */
    public static List<String> readLines(File file) throws IOException {
        return readLines(file, Charset.defaultCharset());
    }

    /**
     * Read all lines from the given file, with a client provided character encoding.
     */
    public static List<String> readLines(File file, Charset charset) throws IOException {
        Preconditions.checkArgument(file.exists(), "File %s not found", file);
        Preconditions.checkArgument(file.isFile(), "%s is not a normal file", file);

        return readLines(new InputStreamReader(new FileInputStream(file), charset));
    }

    /**
     * Read all lines from the provided input stream. The stream is guaranteed to be closed when the function exits.
     */
    public static List<String> readLines(Reader source) throws IOException {
        LineNumberReader lr = new LineNumberReader(source);
        List<String> lines = new ArrayList<>();
        try {
            String line;
            while ((line = lr.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            closeSilently(source);
        }
        return lines;
    }

    /**
     * Get the stream of resource file
     *
     * @param path the relative path to the resource file
     * @return the stream or null if the file does not exist
     */
    public static InputStream getFileResourceAsStream(String path) {
        String resourcePath = path.charAt(0) == '/' ? path : '/' + path;
        InputStream source = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        if (source == null) {
            source = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath.substring(1));
            if (source == null) {
                return null;
            }
        }
        return source;
    }
}
