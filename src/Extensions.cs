using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace GiantBombDataTool
{
    internal static class Extensions
    {
        internal static Stream GetManifestResourceStream(this Type type, string name)
        {
            return type.Assembly.GetManifestResourceStream(type, name);
        }

        internal static string GetManifestResourceString(this Type type, string name)
        {
            using var reader = new StreamReader(GetManifestResourceStream(type, name));
            return reader.ReadToEnd();
        }
    }
}
