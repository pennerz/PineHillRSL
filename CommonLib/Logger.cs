using System;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Diagnostics;

namespace PineRSL.Common
{
    public interface ILog
    {
        void Log(String formatstr, params Object[] args);
    }

    public class FileLog : ILog
    {
        private FileStream _logFile;
        private List<String> _logEntries = new List<String>();

        public FileLog(string logfile)
        {
            _logFile = new FileStream(logfile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
            var newLogEntries = new List<String>();
            Task.Run(async () =>
            {
                while(true)
                {
                    List<String> appendLogs = null;
                    lock (_logFile)
                    {
                        if (_logEntries.Count > 1)
                        {
                            appendLogs = _logEntries;
                            _logEntries = newLogEntries;
                        }
                    }

                    if (appendLogs != null)
                    {
                        newLogEntries = new List<String>();
                        var serializedBuf = new SerializeBuffer();
                        var logBuffs = new List<byte[]>();
                        foreach (var log in appendLogs)
                        {
                            logBuffs.Add(Encoding.ASCII.GetBytes(log));
                        }
                        serializedBuf.AppendBlocks(logBuffs);
                        await _logFile.WriteAsync(serializedBuf.DataBuf, 0, serializedBuf.DataBuf.Length);
                    }

                    await Task.Delay(5000);
                }
            });
        }

        public void Log(String formatstr, params Object?[] args)
        {
            var str = String.Format(formatstr, args);
            str += " ActivityId:" + ActivityControl.CurrentActivityId().ToString() + "\n";
            lock (_logFile)
            {
                _logEntries.Add(str);
            }
        }
    }

    public class ConsoleLog : ILog
    {
        public void Log(String formatstr, params Object?[] args)
        {
            var str = String.Format(formatstr, args);
            str += " ActivityId:" + ActivityControl.CurrentActivityId().ToString() + "\n";
            Console.WriteLine(str);
        }
    }


    public class Logger
    {
        static ILog _loggerImpl = new ConsoleLog();
        static public void Init(ILog logger)
        {
            if (_loggerImpl == null)
            {
                _loggerImpl = logger;
            }
        }

        static public void Log(String formatstr, params Object?[] args)
        {
            _loggerImpl?.Log(formatstr, args);
        }
    }

    public class AutoActivity : IDisposable
    {
        private Guid _prevActivityId = Guid.Empty;

        public AutoActivity(Guid activityid)
        {
            _prevActivityId = Trace.CorrelationManager.ActivityId;
            Trace.CorrelationManager.ActivityId = activityid;
        }

        public void Dispose()
        {
            Trace.CorrelationManager.ActivityId = _prevActivityId;
        }
    }
    public class ActivityControl
    {
        public static AutoActivity NewActivity()
        {
            return new AutoActivity(Guid.NewGuid());
        }

        public static Guid CurrentActivityId()
        {
            return Trace.CorrelationManager.ActivityId;
        }
    }

}
