using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class ListToCSV
    {
        /// <summary>
        /// Create target file
        /// </summary>
        /// <param name="folder">folder</param>
        /// <param name="fileName">folder name</param>
        /// <param name="fileExtension">file extension</param>
        /// <returns>file path</returns>
        public static string CreateFile(string folder, string fileName, string fileExtension="csv")
        {
            FileStream fs = null;
            string filePath = folder + fileName + "." + fileExtension;
            try
            {
                if (!Directory.Exists(folder))
                {
                    Directory.CreateDirectory(folder);
                }
                fs = File.Create(filePath);
            }
            catch (Exception ex)
            { }
            finally
            {
                if (fs != null)
                {
                    fs.Dispose();
                }
            }
            return filePath;
        }

        /// <summary>
        /// Save the List data to CSV file
        /// </summary>
        /// <param name="studentList">data source</param>
        /// <param name="filePath">file path</param>
        /// <returns>success flag</returns>
        public static bool SaveDataToCSVFile<T>(List<T> list, string filePath,string propertyname)
        {
            bool successFlag = true;

            StringBuilder strColumn = new StringBuilder();
            StringBuilder strValue = new StringBuilder();
            StreamWriter sw = null;

            try
            {
                sw = new StreamWriter(filePath);
                strColumn.Append(propertyname);
                sw.WriteLine(strColumn);    //write the column name

                for (int i = 0; i < list.Count; i++)
                {
                    strValue.Remove(0, strValue.Length); //clear the temp row value
                    strValue.Append(list[i]);
                    sw.WriteLine(strValue); //write the row value
                }
            }
            catch (Exception ex)
            {
                successFlag = false;
            }
            finally
            {
                if (sw != null)
                {
                    sw.Dispose();
                }
            }

            return successFlag;
        }

      


    }
}
