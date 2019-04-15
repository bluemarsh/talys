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
                typeof(CloneCommand),
                typeof(FetchCommand),
                typeof(MergeCommand));

            int commandResult = parseResult.MapResult(
                (CloneCommand c) => c.Execute(),
                (FetchCommand c) => c.Execute(),
                (MergeCommand c) => c.Execute(),
                errors => IsHelpOrVersionRequest(errors) ? 0 : 1);

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
