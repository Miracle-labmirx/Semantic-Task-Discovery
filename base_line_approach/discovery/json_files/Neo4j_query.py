# CREATE GENERIC TASK TEMPLATE NAMED WHOLE BUILDING ENERGY
CREATE (t:Task {
  id: "Assessment:A01_Whole-Building-Energy",
  taskId: "A01",
  type: "Task",
  category: "Assessment",
  name: "Whole Building Energy assessment.",
  description: "Takes as input a EUI and the building category. Every day it compares the EUI with the reference values from buildings of the same category.",
  periodicity: ["@daily","@weekly","@monthly","@yearly"]
});

# CREATE GENERIC TASK TEMPLATE FOR KPI NAMED ENERGY USAGE INTENSITY
CREATE (t:Task {
  id: "KPI:Energy-Use-Intensity",
  type: "Task",
  taskId: "A01-K:00001-V01",
  category: "KPI",
  name: "Energy Use Intensity",
  description: "KPI definition for Energy Use Intensity (EUI). Takes as input a timeseries of killowatt/killowatt per hour measurements and the area of the physical space in squared meters and outputs the EUI.",
  periodicity: ["@hourly","@weekly","@monthly","@yearly"]
});

#