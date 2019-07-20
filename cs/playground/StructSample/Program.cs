// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using FASTER.test;

namespace StructSample
{
    public class Program
    {
        static void Main(string[] args)
        {
            new CloneTests().CircularBufferOfClones();
        }
    }
}
