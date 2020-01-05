using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using CommandLine;

namespace Talys
{
    public static class Program
    {
        public static int Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleTraceListener(useErrorStream: true));

            var parseResult = Parser.Default.ParseArguments(
                args,
                new[]
                {
                    typeof(CloneCommand),
                    typeof(FetchCommand),
                    typeof(MergeCommand),
                    typeof(PullCommand),
                    typeof(CompressCommand),
                    typeof(DecompressCommand),
                    typeof(PartitionCommand),
                });

            return parseResult.MapResult<Command, int>(
                command => command.Execute(),
                errors => IsHelpOrVersionRequest(errors) ? 0 : 1);
        }

        private static bool IsHelpOrVersionRequest(IEnumerable<Error> errors)
        {
            return errors.Any(e =>
                e.Tag == ErrorType.HelpRequestedError ||
                e.Tag == ErrorType.HelpVerbRequestedError ||
                e.Tag == ErrorType.VersionRequestedError);
        }
    }
}
