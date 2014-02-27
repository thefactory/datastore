using System;
using System.Collections.Generic;
using System.Linq;
using MonoTouch.Foundation;
using MonoTouch.UIKit;
using MonoTouch.NUnit.UI;
using Splat;
using TheFactory.FileSystem;
using TheFactory.FileSystem.IOS;
using System.Threading.Tasks;

namespace TheFactory.DatastoreTests {

    // The UIApplicationDelegate for the application. This class is responsible for launching the
    // User Interface of the application, as well as listening (and optionally responding) to
    // application events from iOS.
    [Register("UnitTestAppDelegate")]
    public partial class UnitTestAppDelegate : UIApplicationDelegate, IEnableLogger {
        // class-level declarations
        UIWindow window;
        TouchRunner runner;
        //
        // This method is invoked when the application has loaded and is ready to run. In this
        // method you should instantiate the window, load the UI into it and then make the window
        // visible.
        //
        // You have 17 seconds to return from this method, or iOS will terminate your application.
        //
        public override bool FinishedLaunching(UIApplication app, NSDictionary options) {
            // create a new window instance based on the screen size
            window = new UIWindow(UIScreen.MainScreen.Bounds);
            runner = new TouchRunner(window);

            // register every tests included in the main application/assembly
            runner.Add(System.Reflection.Assembly.GetExecutingAssembly());

            window.RootViewController = new UINavigationController(runner.GetViewController());

            // make the window visible
            window.MakeKeyAndVisible();

            // initialize logging and file manager services
            var resolver = Locator.CurrentMutable;
            resolver.RegisterConstant(new ConsoleLogger() { Level = LogLevel.Debug }, typeof(ILogger));
            resolver.RegisterConstant(new IOSFileSystem(), typeof(IFileSystem));

            // dump unobserved exceptions to the console
            TaskScheduler.UnobservedTaskException += (sender, e) => {
                this.Log().DebugException("Unobserved exception", e.Exception);
            };

            return true;
        }

        private class ConsoleLogger : ILogger {
            public void Write(string message, LogLevel logLevel) {
                if (logLevel < Level) {
                    return;
                }
                Console.WriteLine(message);
            }

            public LogLevel Level { get; set; }
        }
    }
}

