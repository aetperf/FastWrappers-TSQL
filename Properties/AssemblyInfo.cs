using System.Reflection;

// Les informations générales relatives à un assembly dépendent du
// jeu d'attributs suivant. Modifiez ces valeurs d'attribut pour modifier les informations
// associées avec l'assembly SQLCLR.
[assembly: AssemblyTitle("FastWrapper")]
[assembly: AssemblyDescription("CLR Wrapper for FastTransfer and FastBCP")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Architecture & Performance")]
[assembly: AssemblyProduct("FastWrappers_TSQL")]
[assembly: AssemblyCopyright("Architecture & Performance")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Les informations de version pour un assembly incluent les quatre valeurs suivantes :
//
//      Version majeure
//      Version mineure 
//     Numéro de build
//      Révision
//
[assembly: AssemblyVersion(FastWrapper.WrapperVersions.Wrapper)]
[assembly: AssemblyFileVersion(FastWrapper.WrapperVersions.Wrapper)]

namespace FastWrapper
{
	// Single source of the versions used by the assembly and by the wrappers.
	internal static class WrapperVersions
	{
		public const string Wrapper = "1.2.0.0";

		// Versions of the binaries the wrappers were validated with
		public const string FastTransfer = "0.18.2";
		public const string FastBCP = "1.6.7";
	}
}
