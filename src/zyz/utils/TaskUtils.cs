using System.Threading.Tasks;
using System.Windows.Forms;

namespace depots.zyz.utils
{
    public class TaskUtils
    {
        public static void text(RichTextBox logBox, string page)
        {
            new Task(() =>
            {
                logBox.AppendText(page);
            }).Start();
            
        }

       
    }
}