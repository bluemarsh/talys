using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using CommandLine;

namespace GiantBombDataTool
{
    static class Program
    {
        static int Main(string[] args)
        {
#if DEBUG
            args = new[] { "clone", "platforms", @"c:\temp\giantbomb" };
#endif

            Trace.Listeners.Add(new ConsoleTraceListener(useErrorStream: true));

            int result = Parser.Default.ParseArguments<CloneCommand, FetchCommand, MergeCommand>(args).MapResult(
                (CloneCommand c) => c.Execute(),
                (FetchCommand c) => c.Execute(),
                (MergeCommand c) => c.Execute(),
                errors => IsHelpOrVersionRequest(errors) ? 0 : 1);

#if DEBUG
            if (Debugger.IsAttached)
            {
                Console.Write("Press any key to continue . . . ");
                Console.Read();
            }
#endif

            return result;
        }

        private static bool IsHelpOrVersionRequest(IEnumerable<Error> errors)
        {
            return errors.Any(e =>
                e.Tag == ErrorType.HelpRequestedError ||
                e.Tag == ErrorType.HelpVerbRequestedError ||
                e.Tag == ErrorType.VersionRequestedError);
        }

        // Not present in .NET Core 2.1 (System.Diagnostics.ConsoleTraceListener)
        class ConsoleTraceListener : TextWriterTraceListener
        {
            public ConsoleTraceListener(bool useErrorStream = false)
                : base(useErrorStream ? Console.Error : Console.Out)
            {
            }

            public override void Close()
            {
                // No resources to clean up.
            }
        }
    }
}
