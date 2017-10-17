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

package io.netflix.titus.common.util.code;

/**
 * {@link CodePointTracker} helps to track places in code that are expected to be unreachable.
 * If a code point is reached (executed), an information about this fact is registered.
 */
public abstract class CodePointTracker {

    private static volatile CodePointTracker INSTANCE;

    public void markReachable() {
        markReachable(getCodePointFromStackFrame(Thread.currentThread().getStackTrace()[2], "<no_context>"));
    }

    public void markReachable(String context) {
        markReachable(getCodePointFromStackFrame(Thread.currentThread().getStackTrace()[2], context));
    }

    protected abstract void markReachable(CodePoint codePoint);

    public static void mark() {
        getDefaultInstance().markReachable(getCodePointFromStackFrame(Thread.currentThread().getStackTrace()[2], "<no_context>"));
    }

    public static void mark(String context) {
        getDefaultInstance().markReachable(getCodePointFromStackFrame(Thread.currentThread().getStackTrace()[2], context));
    }

    public static void setDefault(CodePointTracker codePointTracker) {
        INSTANCE = codePointTracker;
    }

    private static CodePointTracker getDefaultInstance() {
        if (INSTANCE == null) {
            INSTANCE = new LoggingCodePointTracker();
        }
        return INSTANCE;
    }

    private static CodePoint getCodePointFromStackFrame(StackTraceElement callerFrame, String context) {
        return new CodePoint(callerFrame.getClassName(), callerFrame.getMethodName(), callerFrame.getLineNumber(), context);
    }

    static class CodePoint {
        private final String className;
        private final String methodName;
        private final int lineNumber;
        private final String context;

        CodePoint(String className, String methodName, int lineNumber, String context) {
            this.className = className;
            this.methodName = methodName;
            this.lineNumber = lineNumber;
            this.context = context;
        }

        String getClassName() {
            return className;
        }

        String getMethodName() {
            return methodName;
        }

        int getLineNumber() {
            return lineNumber;
        }

        String getContext() {
            return context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CodePoint codePoint = (CodePoint) o;

            if (lineNumber != codePoint.lineNumber) {
                return false;
            }
            if (className != null ? !className.equals(codePoint.className) : codePoint.className != null) {
                return false;
            }
            if (methodName != null ? !methodName.equals(codePoint.methodName) : codePoint.methodName != null) {
                return false;
            }
            return context != null ? context.equals(codePoint.context) : codePoint.context == null;

        }

        @Override
        public int hashCode() {
            int result = className != null ? className.hashCode() : 0;
            result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
            result = 31 * result + lineNumber;
            result = 31 * result + (context != null ? context.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CodePoint{" +
                    "className='" + className + '\'' +
                    ", methodName='" + methodName + '\'' +
                    ", lineNumber=" + lineNumber +
                    ", context='" + context + '\'' +
                    '}';
        }
    }
}
