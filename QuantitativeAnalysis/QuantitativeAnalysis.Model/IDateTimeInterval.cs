using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace QuantitativeAnalysis.Model
{
    public interface IDateTimeInterval:IEnumerator<DateTime>
    {
        DateTime Start { get;  }
        DateTime End { get; }
        DateTime GetNext(DateTime dt);
        DateTime GetPrevious(DateTime dt);
        
    }
}
