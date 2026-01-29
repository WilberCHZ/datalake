# Empresa – proyecto Databricks

Proyecto medallion (bronze / silver / gold) para SAP empresa y personas.

## Despliegue con GitHub Actions

### Requisitos

Configura en tu repositorio de GitHub:

| Tipo    | Nombre            | Descripción                                      |
|--------|--------------------|--------------------------------------------------|
| Variable | `DATABRICKS_HOST` | URL del workspace, p. ej. `https://xxx.cloud.databricks.com` |
| Secret   | `DATABRICKS_TOKEN` | Token de acceso (PAT) de Databricks              |

**Configuración:** Repo → Settings → Secrets and variables → Actions.

### Cómo se despliega

- **Push a `main` o `master`:** se despliega al target `dev`.
- **Manual:** Actions → Deploy to Databricks → Run workflow → eliges `dev` o `prod`.

### Estructura del bundle

- `databricks.yml`: bundle y job `Empresa Gold Reporting`.
- `.github/workflows/deploy.yml`: workflow de validación y despliegue.

El workspace se toma de `DATABRICKS_HOST`; no hace falta configurar host en el bundle.
