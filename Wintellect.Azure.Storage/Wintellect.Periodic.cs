using System;
using System.Diagnostics;


namespace Wintellect.Periodic {
   /// <summary>A static class with helper methods used to calculate when time periods start and end.</summary>
   public static class PeriodCalculator {
      /// <summary>Returns the start date of a cycle.</summary>
      /// <param name="period">The period to use to calculate the period start date.</param>
      /// <param name="cycleDate">Used for MonthlyCycle and YearlyCycle to determine the cross-over date.</param>
      /// <param name="periodsToAdd">The number of periods to add (use a negative number to subtract).</param>
      /// <returns>The period's start date.</returns>
      public static DateTimeOffset CalculatePeriodStartDate(Period period, DateTimeOffset cycleDate, Int32 periodsToAdd = 0) {
         DateTimeOffset periodStop;
         return CalculatePeriodDates(period, cycleDate, periodsToAdd, out periodStop);
      }

      /// <summary>Returns the start date of a cycle.</summary>
      /// <param name="period">The period to use to calculate the period start and end dates.</param>
      /// <param name="cycleDate">Used for MonthlyCycle and YearlyCycle to determine the cross-over date.</param>
      /// <param name="periodsToAdd">The number of periods to add (use a negative number to subtract).</param>
      /// <param name="periodStop">The period's end date.</param>
      /// <returns>The period's start date.</returns>
      public static DateTimeOffset CalculatePeriodDates(Period period, DateTimeOffset cycleDate, Int32 periodsToAdd, out DateTimeOffset periodStop) {
         DateTimeOffset periodStart, today = DateTimeOffset.UtcNow;
         switch (period) {
            case Period.Day: periodStart = periodStop = today.AddDays(periodsToAdd); break;
            case Period.MonthCycle:
               periodStart = GetClosestMonthlyCycleStart(cycleDate).AddMonths(periodsToAdd);
               periodStop = periodStart.AddMonths(1).AddDays(-1);
               break;
            case Period.Month:
               periodStart = new DateTimeOffset(today.Year, today.Month, 1, 0, 0, 0, TimeSpan.Zero).AddMonths(periodsToAdd);
               periodStop = periodStart.AddMonths(1).AddDays(-1);
               break;
            case Period.Quarter:
               periodStart = new DateTimeOffset(today.Year, (((today.Month) / 3) * 3) + 1, 1, 0, 0, 0, TimeSpan.Zero).AddMonths(3 * periodsToAdd);
               periodStop = periodStart.AddMonths(3).AddDays(-1);
               break;
            case Period.YearCycle:
               periodStart = GetClosestYearlyCycleStart(cycleDate).AddYears(periodsToAdd);
               periodStop = periodStart.AddYears(1).AddDays(-1);
               break;
            case Period.Year:
               periodStart = new DateTimeOffset(today.Year, 1, 1, 0, 0, 0, TimeSpan.Zero).AddYears(periodsToAdd);
               periodStop = periodStart.AddYears(1).AddDays(-1);
               break;
            default:
               throw new ArgumentException("Invalid period:" + period, "period");
         }
         return periodStart;
      }
      private static DateTimeOffset GetClosestMonthlyCycleStart(DateTimeOffset date) {
         // Jan 31 -> Feb 28 (or 29 on leap year)
         var today = DateTimeOffset.UtcNow;
         var desiredDate = new DateTime(today.Year, today.Month,
            Math.Min(date.Day, DateTime.DaysInMonth(today.Year, today.Month)));
         if (desiredDate <= today) return desiredDate;

         // If desired is in the future, go back a month (if today is Feb 20 -> Jan 31) 
         today = today.AddMonths(-1);
         return new DateTime(today.Year, today.Month,
            Math.Min(date.Day, DateTime.DaysInMonth(today.Year, today.Month)));
      }

      private static DateTimeOffset GetClosestYearlyCycleStart(DateTimeOffset date) {
         Debug.Assert(date <= DateTimeOffset.UtcNow);
         // Jan 31,2013 -> Feb 28,2014 (or 29 on leap year)
         var today = DateTimeOffset.UtcNow;
         var desiredDate = new DateTime(today.Year, date.Month,
            Math.Min(date.Day, DateTime.DaysInMonth(today.Year, date.Month)));
         if (desiredDate <= today) return desiredDate;

         // If desired is in the future, go back a year
         today = today.AddYears(-1).AddDays(-1);
         return new DateTime(today.Year, date.Month,
            Math.Min(date.Day, DateTime.DaysInMonth(today.Year, date.Month)));
      }
   }

   /// <summary>An enum with the Periods supported by the PeriodCalculator's methods.</summary>
   public enum Period {
      /// <summary>Indicates a daily period.</summary>
      Day, 
      /// <summary>Indicates a monthly period starting at a specific day within a month.</summary>
      MonthCycle,
      /// <summary>Indicates a monthly period starting at the first of the month.</summary>
      Month,
      /// <summary>Indicates a quarterly period.</summary>
      Quarter,
      /// <summary>Indicates a yearly period starting at January 1st.</summary>
      Year,
      /// <summary>Indicates a yearly period starting at a specific day within a year.</summary>
      YearCycle
   }
}