using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace GiantBombDataTool
{
    [Verb("clone")]
    class CloneCommand
    {
        public int Execute()
        {
            Console.WriteLine("Execute Clone");
            return 0;
        }
    }

    [Verb("fetch")]
    class FetchCommand
    {
        public int Execute()
        {
            Console.WriteLine("Execute Fetch");
            return 0;
        }
    }
}
