import { FaBitbucket, FaGithub, FaGitlab, FaAws } from "react-icons/fa6";
import { Provider } from "#/types/settings";

interface GitProviderIconProps {
  gitProvider: Provider;
  className?: string;
}

export function GitProviderIcon({
  gitProvider,
  className,
}: GitProviderIconProps) {
  return (
    <>
      {gitProvider === "github" && <FaGithub size={14} className={className} />}
      {gitProvider === "gitlab" && <FaGitlab className={className} />}
      {gitProvider === "bitbucket" && <FaBitbucket className={className} />}
      {gitProvider === "codecommit" && <FaAws className={className} />}
    </>
  );
}
