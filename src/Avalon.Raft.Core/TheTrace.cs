using Polly;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Avalon.Raft.Core
{
    public static class TheTrace
    {
        private static Action<TraceLevel, string, object[]> _tracer = (level, message, parameters) =>
        {
            var formatted = message;
            try
            {
                formatted = FormatString(message, parameters);
            }
            catch (Exception e)
            {
                formatted += string.Format("\r\n\r\n [!!!FORMATTING THE MESSAGE FAILED (but ignored)!!! = {0}]", e.ToString());
            }

            switch (level)
            {
                case TraceLevel.Error:
                    Trace.TraceError(formatted);
                    break;
                case TraceLevel.Warning:
                    Trace.TraceWarning(formatted);
                    break;
                case TraceLevel.Info:
                case TraceLevel.Verbose:
                    Trace.TraceInformation(formatted);
                    break;
                default:
                    // ignore
                    break;

            }
        };

        private static string FormatString(string messageOrFormat, object[] parameters)
        {
            if (parameters == null || parameters.Length == 0)
                return messageOrFormat;
            else
                return string.Format(messageOrFormat, parameters);
        }

        /// <summary>
        /// Note: Wraps the given in a filter which honours the static Level property
        /// </summary>
        /// <value></value>
        public static Action<TraceLevel, string, object[]> Tracer
        {
            get
            {
                return _tracer;
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");

                _tracer = (l, s, os) => {
                    if (l <= Level)
                        value(l, s, os);
                };
            }
        }

        public static TraceLevel Level {get; set;} = TraceLevel.Verbose;

        public static void TraceError(string message)
        {
            Tracer(TraceLevel.Error, message, new string[0]);
        }

        public static void TraceWarning(string message)
        {
            Tracer(TraceLevel.Warning, message, new string[0]);
        }

        public static void TraceInformation(string message)
        {
            Tracer(TraceLevel.Info, message, new string[0]);
        }

        public static void TraceVerbose(string message)
        {
            Tracer(TraceLevel.Verbose, message, new string[0]);
        }

        public static void TraceError(string message, params object[] parameters)
        {
            Tracer(TraceLevel.Error, message, parameters);
        }

        public static void TraceWarning(string message, params object[] parameters)
        {
            Tracer(TraceLevel.Warning, message, parameters);
        }

        public static void TraceInformation(string message, params object[] parameters)
        {
            Tracer(TraceLevel.Info, message, parameters);
        }

        public static void TraceVerbose(string message, params object[] parameters)
        {
            Tracer(TraceLevel.Verbose, message, parameters);
        }

        public static bool HandleException(Exception e, string extraInfo = null)
        {
            TraceWarning("{0}Handling exception: {1}", extraInfo ?? string.Empty, e);
            return true;
        }

        public static PolicyBuilder LogPolicy(string name)
        {
            return Policy.Handle<Exception>((e) => HandleException(e, $"[{name}] (Polly/Try/Retry) "));
        }
    }

}
