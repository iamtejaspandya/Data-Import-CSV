<?php
namespace Tejas\FormDataImport\Model\Import;

use Exception;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\ImportExport\Helper\Data as ImportHelper;
use Magento\ImportExport\Model\Import;
use Magento\ImportExport\Model\Import\Entity\AbstractEntity;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Magento\ImportExport\Model\ResourceModel\Helper;
use Magento\ImportExport\Model\ResourceModel\Import\Data;

/**
 * Class DataImport
 */
class DataImport extends AbstractEntity
{
    const ENTITY_CODE = 'formdata';
    const TABLE = 'tejas_form_data';
    const ENTITY_ID_COLUMN = 'id';

    /**
     * If we should check column names
     */
    protected $needColumnCheck = true;

    /**
     * Need to log in import history
     */
    protected $logInHistory = true;

    /**
     * Permanent entity columns.
     */
    protected $_permanentAttributes = [
        'id'
    ];

    /**
     * Valid column names
     */
    protected $validColumnNames = [
        'id',
        'first_name',
        'last_name',
        'gender',
        'email',
        'adress1',
        'adress2',
        'city',
        'state',
        'zip_code',
        'feedback'
    ];

    /**
     * @var AdapterInterface
     */
    protected $connection;

    /**
     * @var ResourceConnection
     */
    private $resource;

    /**
     * Courses constructor.
     *
     * @param JsonHelper $jsonHelper
     * @param ImportHelper $importExportData
     * @param Data $importData
     * @param ResourceConnection $resource
     * @param Helper $resourceHelper
     * @param ProcessingErrorAggregatorInterface $errorAggregator
     */
    public function __construct(
        JsonHelper $jsonHelper,
        ImportHelper $importExportData,
        Data $importData,
        ResourceConnection $resource,
        Helper $resourceHelper,
        ProcessingErrorAggregatorInterface $errorAggregator
    ) {
        $this->jsonHelper = $jsonHelper;
        $this->_importExportData = $importExportData;
        $this->_resourceHelper = $resourceHelper;
        $this->_dataSourceModel = $importData;
        $this->resource = $resource;
        $this->connection = $resource->getConnection(ResourceConnection::DEFAULT_CONNECTION);
        $this->errorAggregator = $errorAggregator;
        $this->initMessageTemplates();
    }

    /**
     * Entity type code getter.
     *
     * @return string
     */
    public function getEntityTypeCode()
    {
        return static::ENTITY_CODE;
    }

    /**
     * Get available columns
     *
     * @return array
     */
    public function getValidColumnNames(): array
    {
        return $this->validColumnNames;
    }

    /**
     * Row validation
     *
     * @param array $rowData
     * @param int $rowNum
     *
     * @return bool
     */
    public function validateRow(array $rowData, $rowNum): bool
    {
        $first_name = $rowData['first_name'] ?? '';
        $last_name = $rowData['last_name'] ?? '';
        $gender = $rowData['gender'] ?? '';
        $email = $rowData['email'] ?? '';
        $adress1 = $rowData['adress1'] ?? '';
        $adress2 = $rowData['adress2'] ?? '';
        $city = $rowData['city'] ?? '';
        $state = $rowData['state'] ?? '';
        $zip_code = $rowData['zip_code'] ?? '';
        $feedback = $rowData['feedback'] ?? '';
        
        if (!$first_name) {
            $this->addRowError('FirstNameIsRequired', $rowNum);
        }
        
        if (!$last_name) {
            $this->addRowError('LastNameIsRequired', $rowNum);
        }

        if (!$gender) {
            $this->addRowError('GenderIsRequired', $rowNum);
        }
        
        if (!$email) {
            $this->addRowError('EmailIsRequired', $rowNum);
        }

        if (!$adress1) {
            $this->addRowError('Adress1IsRequired', $rowNum);
        }
        
        if (!$adress2) {
            $this->addRowError('Adress2IsRequired', $rowNum);
        }

        if (!$city) {
            $this->addRowError('CityIsRequired', $rowNum);
        }
        
        if (!$state) {
            $this->addRowError('StateIsRequired', $rowNum);
        }

        if (!$zip_code) {
            $this->addRowError('ZipCodeIsRequired', $rowNum);
        }
        
        if (!$feedback) {
            $this->addRowError('FeedbackIsRequired', $rowNum);
        }

        if (isset($this->_validatedRows[$rowNum])) {
            return !$this->getErrorAggregator()->isRowInvalid($rowNum);
        }

        $this->_validatedRows[$rowNum] = true;

        return !$this->getErrorAggregator()->isRowInvalid($rowNum);
    }

    /**
     * Init Error Messages
    */
    private function initMessageTemplates()
    {
        $this->addMessageTemplate(
            'FirstNameIsRequired',
            __('The First Name cannot be empty.')
        );
        $this->addMessageTemplate(
            'LastNameIsRequired',
            __('The Last Name cannot be empty.')
        );
        $this->addMessageTemplate(
            'GenderIsRequired',
            __('The Gender cannot be empty.')
        );
        $this->addMessageTemplate(
            'EmailIsRequired',
            __('The Email cannot be empty.')
        );
        $this->addMessageTemplate(
            'Adress1IsRequired',
            __('The Adress 1 cannot be empty.')
        );
        $this->addMessageTemplate(
            'Adress2IsRequired',
            __('The Adress 2 cannot be empty.')
        );
        $this->addMessageTemplate(
            'CityIsRequired',
            __('The City cannot be empty.')
        );
        $this->addMessageTemplate(
            'StateIsRequired',
            __('The State cannot be empty.')
        );
        $this->addMessageTemplate(
            'ZipCodeIsRequired',
            __('The Zip Code cannot be empty.')
        );
        $this->addMessageTemplate(
            'FeedbackIsRequired',
            __('The Feedback cannot be empty.')
        );
    }
    
    /**
     * Import data
     *
     * @return bool
     *
     * @throws Exception
     */
    protected function _importData(): bool
    {
        switch ($this->getBehavior()) {
            case Import::BEHAVIOR_DELETE:
                $this->deleteEntity();
                break;
            case Import::BEHAVIOR_REPLACE:
                $this->saveAndReplaceEntity();
                break;
            case Import::BEHAVIOR_APPEND:
                $this->saveAndReplaceEntity();
                break;
        }

        return true;
    }

    /**
     * Delete entities
     *
     * @return bool
     */
    private function deleteEntity(): bool
    {
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            foreach ($bunch as $rowNum => $rowData) {
                $this->validateRow($rowData, $rowNum);

                if (!$this->getErrorAggregator()->isRowInvalid($rowNum)) {
                    $rowId = $rowData[static::ENTITY_ID_COLUMN];
                    $rows[] = $rowId;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                }
            }
        }

        if ($rows) {
            return $this->deleteEntityFinish(array_unique($rows));
        }

        return false;
    }

    /**
     * Save and replace entities
     *
     * @return void
     */
    private function saveAndReplaceEntity()
    {
        $behavior = $this->getBehavior();
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $entityList = [];

            foreach ($bunch as $rowNum => $row) {
                if (!$this->validateRow($row, $rowNum)) {
                    continue;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);

                    continue;
                }

                $rowId = $row[static::ENTITY_ID_COLUMN];
                $rows[] = $rowId;
                $columnValues = [];

                foreach ($this->getAvailableColumns() as $columnKey) {
                    $columnValues[$columnKey] = $row[$columnKey];
                }

                $entityList[$rowId][] = $columnValues;
                $this->countItemsCreated += (int) !isset($row[static::ENTITY_ID_COLUMN]);
                $this->countItemsUpdated += (int) isset($row[static::ENTITY_ID_COLUMN]);
            }

            if (Import::BEHAVIOR_REPLACE === $behavior) {
                if ($rows && $this->deleteEntityFinish(array_unique($rows))) {
                    $this->saveEntityFinish($entityList);
                }
            } elseif (Import::BEHAVIOR_APPEND === $behavior) {
                $this->saveEntityFinish($entityList);
            }
        }
    }

    /**
     * Save entities
     *
     * @param array $entityData
     *
     * @return bool
     */
    private function saveEntityFinish(array $entityData): bool
    {
        if ($entityData) {
            $tableName = $this->connection->getTableName(static::TABLE);
            $rows = [];

            foreach ($entityData as $entityRows) {
                foreach ($entityRows as $row) {
                    $rows[] = $row;
                }
            }

            if ($rows) {
                $this->connection->insertOnDuplicate($tableName, $rows, $this->getAvailableColumns());

                return true;
            }
            return false;
        }
        return false;
    }

    /**
     * Delete entities
     *
     * @param array $entityIds
     *
     * @return bool
     */
    private function deleteEntityFinish(array $entityIds): bool
    {
        if ($entityIds) {
            try {
                $this->countItemsDeleted += $this->connection->delete(
                    $this->connection->getTableName(static::TABLE),
                    $this->connection->quoteInto(static::ENTITY_ID_COLUMN . ' IN (?)', $entityIds)
                );

                return true;
            } catch (Exception $e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Get available columns
     *
     * @return array
     */
    private function getAvailableColumns(): array
    {
        return $this->validColumnNames;
    }

    public function customLogger()
    {
        $writer = new \Zend_Log_Writer_Stream(BP . '/var/log/custom_test.log');
        $logger = new \Zend_Log();
        $logger->addWriter($writer);
        return $logger;
    }
}
