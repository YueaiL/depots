using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace depots.src.zyz.pojo
{
    class TableStruct
    {
        public string name { get; set; }
        public string remark { get; set; }
        public string type { get; set; }
        public string len { get; set; }
        public string lessLen { get; set; }
        public int bs { get; set; }
        public int main { get; set; }
        public int isnull { get; set; }
        public string defaultVal { get; set; }
    }
}
