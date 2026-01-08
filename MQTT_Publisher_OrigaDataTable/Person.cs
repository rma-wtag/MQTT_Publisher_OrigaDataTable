using System;
using System.Collections.Generic;
using System.Text;

namespace MQTT_Publisher_OrigaDataTable
{
    public class Person
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public int Age { get; set; }
        public bool IsActive { get; set; }
    }
}
