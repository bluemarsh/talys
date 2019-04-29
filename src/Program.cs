using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using CommandLine;

namespace GiantBombDataTool
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
                });

            int commandResult = 1;
            parseResult
                .WithParsed<Command>(c => commandResult = c.Execute())
                .WithNotParsed(errors => commandResult = IsHelpOrVersionRequest(errors) ? 0 : 1);

            return commandResult;
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
