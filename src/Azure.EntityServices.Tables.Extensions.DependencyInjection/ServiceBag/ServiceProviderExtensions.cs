using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{ 
    public static class ServiceProviderExtensions
    { 
        public static T With<S1, T>(this IServiceProvider provider, Func<S1, T> factory)=>
        factory.Invoke(provider.GetRequiredService<S1>()); 

        public static T With<S1, S2, T>(this IServiceProvider provider, Func<S1, S2, T> factory)=>
         factory.Invoke(provider.GetRequiredService<S1>(), provider.GetRequiredService<S2>()); 

        public static T  With<S1, S2, S3, T>(this IServiceProvider provider, Func<S1, S2, S3, T> factory)=>
         factory.Invoke(
                provider.GetRequiredService<S1>(),
                provider.GetRequiredService<S2>(),
                provider.GetRequiredService<S3>()); 

        public static T With<S1, S2, S3, S4, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, T> factory)=>
         factory.Invoke(
               provider.GetRequiredService<S1>(),
               provider.GetRequiredService<S2>(),
               provider.GetRequiredService<S3>(),
               provider.GetRequiredService<S4>()); 

        public static T With<S1, S2, S3, S4, S5, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, S5, T> factory)=>
        factory.Invoke(
                provider.GetRequiredService<S1>(),
                provider.GetRequiredService<S2>(),
                provider.GetRequiredService<S3>(),
                provider.GetRequiredService<S4>(),
                provider.GetRequiredService<S5>()); 

        public static T With<S1, S2, S3, S4, S5, S6, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, S5, S6, T> factory)=>
        factory.Invoke(
              provider.GetRequiredService<S1>(),
              provider.GetRequiredService<S2>(),
              provider.GetRequiredService<S3>(),
              provider.GetRequiredService<S4>(),
              provider.GetRequiredService<S5>(),
              provider.GetRequiredService<S6>());
     
        public static T With<S1, S2, S3, S4, S5, S6, S7, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, S5, S6, S7, T> factory)=>
           factory.Invoke(
              provider.GetRequiredService<S1>(),
              provider.GetRequiredService<S2>(),
              provider.GetRequiredService<S3>(),
              provider.GetRequiredService<S4>(),
              provider.GetRequiredService<S5>(),
              provider.GetRequiredService<S6>(),
              provider.GetRequiredService<S7>());
     

        public static T With<S1, S2, S3, S4, S5, S6, S7, S8, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, S5, S6, S7, S8, T> factory) =>
         factory.Invoke(
              provider.GetRequiredService<S1>(),
              provider.GetRequiredService<S2>(),
              provider.GetRequiredService<S3>(),
              provider.GetRequiredService<S4>(),
              provider.GetRequiredService<S5>(),
              provider.GetRequiredService<S6>(),
              provider.GetRequiredService<S7>(),
              provider.GetRequiredService<S8>()); 

        public static T With<S1, S2, S3, S4, S5, S6, S7, S8, S9, T>(this IServiceProvider provider, Func<S1, S2, S3, S4, S5, S6, S7, S8, S9, T> factory)=>
           factory.Invoke(
              provider.GetRequiredService<S1>(),
              provider.GetRequiredService<S2>(),
              provider.GetRequiredService<S3>(),
              provider.GetRequiredService<S4>(),
              provider.GetRequiredService<S5>(),
              provider.GetRequiredService<S6>(),
              provider.GetRequiredService<S7>(),
              provider.GetRequiredService<S8>(),
              provider.GetRequiredService<S9>()); 
    }
}