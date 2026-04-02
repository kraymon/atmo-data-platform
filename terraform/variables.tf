variable "location" {
  description = "Région Azure"
  type        = string
  default     = "francecentral"
}

variable "resource_group_name" {
  description = "Nom du resource group"
  type        = string
  default     = "atmo-rg"
}

variable "storage_account_name" {
  description = "Nom du storage account (unique globalement, 3-24 chars, minuscules)"
  type        = string
}

variable "vm_admin_username" {
  description = "Nom d'utilisateur admin de la VM"
  type        = string
  default     = "atmo"
}

variable "vm_size" {
  description = "Taille de la VM"
  type        = string
  default     = "Standard_B2ms"
}

variable "ssh_public_key_path" {
  description = "Chemin vers la clé publique SSH"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}